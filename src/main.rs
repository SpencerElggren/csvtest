use std::error::Error;
use std::io;
use std::process::exit;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Deserialize, Clone, Copy)]
struct Transaction<'a> {
    // trx type processed as bits to bypass utf-8 check for optimization
    trx_type: &'a [u8],
    client: u16,
    tx: u32,
    amount: f32,
}

#[derive(Debug, Clone)]
struct ClientSummary<'a> {
    client: u16,
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
    // making previous transaction part of ClientSummary negates need to loop over ALL previous tx
    prev_transactions: Vec<Transaction<'a>>
}

#[derive(Debug, Serialize, Clone, Copy)]
struct ClientPrint {
    // To print, the prev_transactions are not required
    client: u16,
    available: f32,
    held: f32,
    total: f32,
    locked: bool
}
// read csv files
fn create_transactions(mut client_list: Vec<ClientSummary>) -> Result<Vec<ClientSummary>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let mut raw_record = csv::ByteRecord::new();
    let headers = rdr.byte_headers()?.clone();

    while rdr.read_byte_record(&mut raw_record)? {
        let transaction: Transaction = raw_record.deserialize(Some(&headers))?;
        // not saving a vector of transactions and instead adding data as streamed to client list
        create_clients_vector(transaction, &mut client_list)?;
    }
    Ok(client_list)
}

// create map and add to client as one pass through list of transactions, creating new clients as they appear

fn create_clients_vector<'a>(transaction: Transaction, client_list: &mut Vec<ClientSummary<'_>>) -> Result<(), Box<dyn Error>> {

    // reading transactions as streamed, here creating and updating client data
    for mut client in client_list.clone() {
        if client.client == transaction.client {
            client.prev_transactions.push(transaction);
            match transaction.trx_type {
                b"deposit" => client.available += transaction.amount,
                b"withdrawal" => {
                    client.available -= transaction.amount;
                    client.total -= transaction.amount;
                },
                b"dispute" => {
                    for transaction_prev in client.prev_transactions {
                        if transaction_prev.tx == transaction.tx {
                            client.available -= transaction_prev.amount;
                            client.held += transaction_prev.amount;
                            continue;
                        }
                    }
                }
                b"resolve" => {
                    for transaction_prev in client.prev_transactions {
                        if transaction_prev.tx == transaction.tx {
                            client.held -= transaction_prev.amount;
                            client.available += transaction_prev.amount;
                            continue;
                        }
                    }
                }
                b"chargeback" => {
                    for transaction_prev in client.prev_transactions {
                        if transaction_prev.tx == transaction.tx {
                            client.available -= transaction_prev.amount;
                            client.held += transaction_prev.amount;
                            client.locked = true;
                            continue;
                        }
                    }
                }
                _ => continue
            }
        } else {
            client_list.push(ClientSummary {
                client: transaction.client,
                available: transaction.amount,
                held: 0.0,
                total: transaction.amount,
                locked: false,
                prev_transactions: vec![]
            });
        }
        // assumption that a only a deposit can be the first transaction for a client
    }

    Ok(())
}

// output should be written as std out
// write without transaction list of client
fn write(clients: Result<Vec<ClientSummary>, Box<dyn Error>>) -> Result<(), Box<dyn Error>> {
    let mut wtr = csv::Writer::from_writer(io::stdout());
    for client in clients.expect("failed to serialize") {
        wtr.serialize(ClientPrint{
            client: client.client,
            available: client.available,
            held: client.held,
            total: client.total,
            locked: client.locked,
        })?;
    }

    wtr.flush()?;
    Ok(())
}

fn main() {
    let client_list: Vec<ClientSummary> = vec![];
    let new_list = create_transactions(client_list);

    if let Err(err) = write(new_list) {
        println!("{}", err)
    }

    exit(1);
}
