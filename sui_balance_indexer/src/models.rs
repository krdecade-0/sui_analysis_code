use diesel::prelude::*;

use crate::schema::balance_changes;

#[derive(Debug, Clone, Queryable, Identifiable)]
#[diesel(table_name = balance_changes)]
#[diesel(primary_key(id))]
pub struct BalanceChange {
    pub id: i64,
    pub tx_digest: Vec<u8>,
    pub coin_type: String,
    pub amount: i64,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = balance_changes)]
pub struct NewBalanceChange {
    pub tx_digest: Vec<u8>,
    pub coin_type: String,
    pub amount: i64,
}