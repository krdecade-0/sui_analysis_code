// @generated automatically by Diesel CLI.

diesel::table! {
    balance_changes (id) {
        id -> Int8,
        tx_digest -> Bytea,
        coin_type -> Text,
        amount -> Int8,
    }
}
