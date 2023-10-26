use bson::doc;
use mongodb::{IndexModel, Collection};
use mongodb::{options::ClientOptions, Client};
use tokio::task::JoinSet;
use std::time::{Instant, Duration};
use std::error::Error;
use std::{thread, time};

fn create_dynamic_document(num_fields: i32) -> bson::Document {
    let mut document: bson::Document = bson::Document::new();

    for i in 1..=num_fields {
        let field_name: String = format!("field{}", i);
        let field_value: String = format!("value{}", i);
        document.insert(field_name, field_value);
    }

    document
}

async fn get_client() -> Result<Client, mongodb::error::Error> {
    let mut client_options: ClientOptions = ClientOptions::parse("mongodb+srv://<user>:<password>@<host>/?retryWrites=true&w=majority").await?;
    client_options.app_name = Some("Benchmark".to_string());
    let client: Client = Client::with_options(client_options)?;

    Ok(client)
}

async fn insert_documents(number_of_insertions: i32, index: i32, collection: &Collection<bson::Document>) -> Result<u128, mongodb::error::Error> {

    let new_doc: bson::Document = create_dynamic_document(64);

    let mut execution_times: Vec<Duration> = Vec::new();
    let mut set: JoinSet<Duration> = JoinSet::new();

    if index > 0 {

        let index_options: IndexModel = IndexModel::builder()
            .keys(doc! {format!("field{}", index): 1 })
            .build();

        collection.create_index(index_options, None).await?;
    }
    
    for _ in 0..number_of_insertions {
        let new_doc_clone: bson::Document = new_doc.clone();
        let collection_clone: mongodb::Collection<bson::Document> = collection.clone();
        set.spawn(async move {
            let start: Instant = Instant::now();
            let _ = collection_clone.insert_one(new_doc_clone, None).await;
            let duration: Duration = start.elapsed();
            duration
        });
    }

    while let Some(res) = set.join_next().await {
        let duration: Duration = res.unwrap();
        execution_times.push(duration)
    }

    let total_duration: Duration = execution_times.iter().sum();
    let average_duration: Duration = total_duration / execution_times.len() as u32;

    Ok(average_duration.as_millis())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let client: Client = get_client().await?;

    let db: mongodb::Database = client.database("test_db");
    let collection: mongodb::Collection<_> = db.collection("test_collection");
    let collection_stats: mongodb::Collection<_> = db.collection("stats_collection");
    
    collection.drop_indexes(None).await?;

    let number_of_insertions: i32 = 100;
    let delay_time: Duration = time::Duration::from_secs(120);

    for n in 0..2  {
        let result: u128 = insert_documents(number_of_insertions, n, &collection).await?;
        println!("Insercao de {} documentos com {} indices, demorou - {}ms", number_of_insertions, n, result);

        collection.delete_many(doc! {}, None).await?;

        collection_stats.insert_one(doc! {"docs": number_of_insertions, "indexes": n, "time": result as u32}, None).await?;

        // delay para normaliza o uso de CPU e etc ...
        println!("Aguardando {} segundos para proxima insercao", delay_time.as_secs());
        thread::sleep(delay_time);
    }

    Ok(())
}