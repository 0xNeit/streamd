module coverage::coverage {
    use std::signer;
    use aptos_framework::event;

    struct Config has key { bumps: u64 }

    #[event]
    struct Created has drop, store { publisher: address }

    #[event]
    struct Bumped has drop, store { who: address, value: u64 }

    #[event]
    struct Tagged<phantom T> has drop, store { who: address }

    #[event]
    struct Never has drop, store { reason: u64 }

    fun init_module(publisher: &signer) {
        move_to(publisher, Config { bumps: 0 });
        event::emit(Created { publisher: signer::address_of(publisher) });
    }

    public entry fun bump(account: &signer, value: u64) acquires Config {
        let config = borrow_global_mut<Config>(@coverage);
        config.bumps = config.bumps + 1;
        event::emit(Bumped { who: signer::address_of(account), value });
    }

    public entry fun tag<T>(account: &signer) {
        event::emit(Tagged<T> { who: signer::address_of(account) });
    }

    public entry fun never(_account: &signer) {
        if (false) event::emit(Never { reason: 0 });
    }
}
