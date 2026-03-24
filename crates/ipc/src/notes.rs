//! Notes on the ideal interface goal

use mio::Registry;

use crate::token::TokenRegistry;

fn ideal_main() {
    // Here the idea is that we could make a blueprint that then will statically type and specialize our IPC layer.
    // This is not the current implementation, but it is the ideal interface goal.
    let blueprint = make_blueprint! {
        MARKET_DATA: kind=Broadcast, role=Connect, capacity=1024, type="MarketData",
        TARGET_PORTFOLIO: kind=Aggregate, role=Connect, capacity=1024, type="TargetPortfolio",
        LOGGER: kind=Aggregate, role=Connect, capacity=1024, type="JournalEvent",
    };

    // Actually, instead of a blueprint, we could call it make ipc
    // and build the runner and the handle in one as follow.
    let (ipc_runner, blueprint, market_data, target_portfolio) = make_ipc! {
        MARKET_DATA: kind=Broadcast, role=Connect, capacity=1024, ty=MarketData, addr="[IP_ADDRESS]:9000", transport=TCP,
        TARGET_PORTFOLIO: kind=Aggregate, role=Connect, capacity=1024, ty=TargetPortfolio, addr="[IP_ADDRESS]:9001", transport=TCP,
    };

    thread_pool::spawn(ipc_runner);

    // The goal if for the code above to be extended to look as follow

    let (engine, blueprint, market_data, target_portfolio) = {
        let mut token_prefix = 0;

        // The macro uses standard traits and ZERO-SIZED TUPLES to dynamically
        // fetch the highly optimized, perfectly-sized engine structs!
        let (engine_MARKET_DATA, handle_MARKET_DATA) =
            <(Broadcast, Connect, TCP) as SocketFactory<MarketData>>::build(
                addr,
                capacity,
                TokenRegistry::new(token_prefix),
            );
        token_prefix += 1;

        let (engine_TARGET_PORTFOLIO, handle_TARGET_PORTFOLIO) =
            <(Aggregate, Connect, TCP) as SocketFactory<TargetPortfolio>>::build(
                addr,
                capacity,
                TokenRegistry::new(token_prefix),
            );
        token_prefix += 1;

        let engine = (engine_MARKET_DATA, engine_TARGET_PORTFOLIO);

        (
            engine,
            engine_blueprint, // made before but I'm lazy to write it yet
            handle_MARKET_DATA,
            handle_TARGET_PORTFOLIO,
        )
    };
}
