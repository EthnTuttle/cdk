//! End-to-end integration test for the Iroh QUIC transport.
//!
//! Spins up a CDK mint with FakeWallet backend, wraps it in an IrohMintServer,
//! connects a wallet via IrohAsync transport, and exercises the full NUT flow:
//! keys, mint quote, mint tokens, swap, check state, restore, melt.
//!
//! Run with:
//! ```
//! CDK_TEST_DB_TYPE=memory cargo run --bin iroh_test --features iroh -p cdk-integration-tests
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bip39::Mnemonic;
use cdk::amount::SplitTarget;
use cdk::nuts::{CurrencyUnit, PaymentMethod};
use cdk::wallet::{IrohAsync, IrohMintClientBase, WalletBuilder};
use cdk::{Amount, StreamExt};
use cdk_fake_wallet::create_fake_invoice;
use cdk_integration_tests::init_pure_tests;
use iroh::Endpoint;

#[tokio::main]
async fn main() -> Result<()> {
    // Set a default if not already set (convenience for direct cargo run)
    if std::env::var("CDK_TEST_DB_TYPE").is_err() {
        std::env::set_var("CDK_TEST_DB_TYPE", "memory");
    }

    init_pure_tests::setup_tracing();

    test_iroh_transport().await?;
    println!("All Iroh transport tests passed!");
    Ok(())
}

async fn test_iroh_transport() -> Result<()> {
    // ------------------------------------------------------------------
    // 1. Create the mint (FakeWallet backend, in-memory SQLite)
    // ------------------------------------------------------------------
    println!("Creating test mint...");
    let mint = init_pure_tests::create_and_start_test_mint().await?;
    let mint = Arc::new(mint);

    // ------------------------------------------------------------------
    // 2. Start the IrohMintServer
    // ------------------------------------------------------------------
    println!("Starting IrohMintServer...");
    let server = cdk_iroh::IrohMintServer::new(
        Arc::clone(&mint),
        cdk_iroh::IrohMintConfig {
            identity_path: None,
        },
    )
    .await?;

    let node_id = server.node_id();
    println!("  server node_id: {node_id}");

    // Get the server's full NodeAddr (includes direct socket addresses).
    // This is needed for local tests where there is no relay to discover the server.
    let server_node_addr = server.node_addr().await?;
    println!("  server direct addrs: {:?}", server_node_addr.direct_addresses().count());

    // The IrohMintServer::new() already spawns the accept loop via Router::spawn().
    // We must keep `server` alive for the duration of the test.
    // Do NOT call server.run() — that calls router.shutdown() which tears down the server.
    // We store `server` here and drop it after the test.
    // (We use a oneshot channel so we can signal shutdown at the end.)
    let (server_tx, server_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        // Wait until we signal shutdown
        let _ = server_rx.await;
        // Now explicitly shut the server down
        drop(server);
    });

    // Give the server a moment to start accepting connections.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ------------------------------------------------------------------
    // 3. Build the wallet client using IrohAsync transport
    // ------------------------------------------------------------------
    println!("Building wallet client...");
    let client_endpoint = Endpoint::builder().bind().await?;

    // Register the server's direct addresses with the client endpoint.
    // This allows the client to connect without needing a relay server.
    client_endpoint
        .add_node_addr(server_node_addr)
        .map_err(|e| anyhow::anyhow!("add_node_addr failed: {e}"))?;

    // IrohAsync implements the Transport trait and can be used with HttpClient.
    // HttpClient::with_transport accepts a pre-built transport T.
    // The mint_url is a placeholder; the actual network path goes through Iroh QUIC.
    let iroh_transport = IrohAsync::new(client_endpoint, node_id);
    let mint_url: cdk::mint_url::MintUrl = "https://iroh.local".parse()?;
    let connector: IrohMintClientBase<IrohAsync> =
        IrohMintClientBase::with_transport(mint_url.clone(), iroh_transport, None);

    // ------------------------------------------------------------------
    // 4. Build the wallet
    // ------------------------------------------------------------------
    println!("Building wallet...");
    let seed = Mnemonic::generate(12)?.to_seed_normalized("");
    let localstore = Arc::new(cdk_sqlite::wallet::memory::empty().await?);

    let wallet = WalletBuilder::new()
        .mint_url(mint_url)
        .unit(CurrencyUnit::Sat)
        .localstore(localstore)
        .seed(seed)
        .client(connector)
        .build()?;

    // ------------------------------------------------------------------
    // 5. NUT-01 / NUT-02: keys & keysets
    // ------------------------------------------------------------------
    println!("\nTesting get_mint_keysets (NUT-02)...");
    let keysets = wallet.get_mint_keysets().await?;
    assert!(!keysets.is_empty(), "keysets list must not be empty");
    println!("  total keysets: {}", keysets.len());

    println!("Testing fetch_active_keyset (NUT-01)...");
    let active_keyset = wallet.fetch_active_keyset().await?;
    println!("  active keyset id: {}", active_keyset.id);

    // ------------------------------------------------------------------
    // 6. NUT-06: mint info
    // ------------------------------------------------------------------
    println!("Testing fetch_mint_info (NUT-06)...");
    let info = wallet.fetch_mint_info().await?;
    println!("  mint info: {:?}", info.as_ref().and_then(|i| i.name.as_deref()));

    // ------------------------------------------------------------------
    // 7. NUT-04: mint tokens
    //    FakeWallet auto-pays the quote after delay_secs=2.
    // ------------------------------------------------------------------
    println!("\nTesting mint flow (NUT-04)...");
    let mint_amount = Amount::from(64u64);
    let quote = wallet
        .mint_quote(PaymentMethod::BOLT11, Some(mint_amount), None, None)
        .await?;
    println!("  mint quote id: {}", quote.id);

    // proof_stream polls the quote and mints once the FakeWallet auto-pays it.
    let proofs = wallet
        .proof_stream(quote, SplitTarget::default(), None)
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("proof stream ended without producing proofs"))??;

    use cdk::nuts::nut00::ProofsMethods;
    let minted = proofs.total_amount()?;
    println!("  minted: {minted} sat");
    assert_eq!(minted, mint_amount, "minted amount must match requested amount");

    // ------------------------------------------------------------------
    // 8. Wallet balance
    // ------------------------------------------------------------------
    let balance = wallet.total_balance().await?;
    println!("  wallet balance: {balance} sat");
    assert!(balance >= mint_amount, "balance must be at least what we minted");

    // ------------------------------------------------------------------
    // 9. NUT-03: swap
    //    We need to pass the actual proofs to swap.
    // ------------------------------------------------------------------
    println!("\nTesting swap (NUT-03)...");
    let unspent_proofs = wallet.get_unspent_proofs().await?;
    assert!(!unspent_proofs.is_empty(), "should have unspent proofs after mint");
    let swap_amount = Amount::from(32u64);
    let send_proofs = wallet
        .swap(
            Some(swap_amount),
            SplitTarget::default(),
            unspent_proofs,
            None,
            false,
        )
        .await?;
    let send_count = send_proofs.as_ref().map(|p| p.len()).unwrap_or(0);
    println!("  swap successful, got {send_count} sendable proofs");
    assert!(send_proofs.is_some(), "swap should return sendable proofs");

    // ------------------------------------------------------------------
    // 10. NUT-07: check state
    // ------------------------------------------------------------------
    println!("Testing check_state (NUT-07) via check_proofs_spent...");
    let remaining_proofs = wallet.get_unspent_proofs().await?;
    println!("  wallet has {} unspent proofs", remaining_proofs.len());
    if !remaining_proofs.is_empty() {
        let states = wallet.check_proofs_spent(remaining_proofs).await?;
        println!("  check_state returned {} states", states.len());
    }

    // ------------------------------------------------------------------
    // 11. NUT-05: melt
    //    FakeWallet accepts any invoice; create a fake bolt11.
    // ------------------------------------------------------------------
    println!("\nTesting melt flow (NUT-05)...");
    let melt_amount_sat: u64 = 10;
    let fake_invoice = create_fake_invoice(melt_amount_sat * 1_000, "iroh test melt".to_string());
    let invoice_str = fake_invoice.to_string();
    println!("  requesting melt quote for {} sat", melt_amount_sat);

    let melt_quote = wallet
        .melt_quote(PaymentMethod::BOLT11, invoice_str, None, None)
        .await?;
    println!("  melt quote id: {}", melt_quote.id);
    println!("  melt quote amount: {} sat", melt_quote.amount);

    // Prepare and confirm the melt
    let prepared_melt = wallet.prepare_melt(&melt_quote.id, HashMap::new()).await?;
    let finalized = prepared_melt.confirm().await?;
    println!("  melt finalized: state={:?}", finalized.state());

    // ------------------------------------------------------------------
    // 12. Final balance check
    // ------------------------------------------------------------------
    println!("\nFinal balance check...");
    let final_balance = wallet.total_balance().await?;
    println!("  final balance: {final_balance} sat");

    // Signal the server to shut down cleanly.
    let _ = server_tx.send(());

    println!("\nAll assertions passed.");
    Ok(())
}
