module counter::counter {
    use std::signer;
    use aptos_framework::event;

    #[event]
    struct Bumped has drop, store { who: address, value: u64 }

    public entry fun bump(account: &signer, value: u64) {
        event::emit(Bumped { who: signer::address_of(account), value });
    }
}
