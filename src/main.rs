use bson::doc;
use mongodb::{IndexModel, Collection};
use mongodb::{options::ClientOptions, Client};
use tokio::task::JoinSet;
use std::time::{Instant, Duration};
use std::error::Error;
use std::{thread, time, env};
use rand::Rng;

fn generate_random_string(length: usize) -> String {
    let charset: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let charset_length: usize = charset.len();
    let mut rng: rand::rngs::ThreadRng = rand::thread_rng();

    let random_string: String = (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..charset_length);
            charset.chars().nth(idx).unwrap()
        })
        .collect();

    random_string
}

fn create_dynamic_document(num_fields: i32) -> bson::Document {
    let mut document: bson::Document = bson::Document::new();

    for i in 1..=num_fields {
        let field_name: String = format!("field{}", i);
        let field_value: String = generate_random_string(20);
        document.insert(field_name, field_value);
    }

    document
}

async fn get_client() -> Result<Client, mongodb::error::Error> {
    let mut client_options: ClientOptions = ClientOptions::parse("mongodb+srv://<user>:<password>@<host>/?retryWrites=true&w=1").await?;
    client_options.app_name = Some("Benchmark".to_string());
    let client: Client = Client::with_options(client_options)?;

    Ok(client)
}

async fn insert_documents(number_of_insertions: i32, collection: &Collection<bson::Document>) -> Result<u128, mongodb::error::Error> {

    let new_doc: bson::Document = create_dynamic_document(64);

    let mut execution_times: Vec<Duration> = Vec::new();
    let mut set: JoinSet<Duration> = JoinSet::new();
    
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

async fn pre_load(number_of_thread: i32, number_of_insertions: i32, collection: &Collection<bson::Document>) -> () {

    println!("Start loading, {} threads com {} documentos", number_of_thread, number_of_insertions);

    let mut set: JoinSet<()> = JoinSet::new();
    
    for _ in 0..number_of_thread {

        let mut docs: Vec<bson::Document> = Vec::new();
        for _ in 0..number_of_insertions {
            docs.push(create_dynamic_document(64));
        }

        let collection_clone: mongodb::Collection<bson::Document> = collection.clone();
        set.spawn(async move {
            let _ = collection_clone.insert_many(docs, None).await;
        });
    }

    while let Some(res) = set.join_next().await {
        let _ = res.unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        return Err(format!("Uso: {} test OU {} load <threads> <nDocs>", args[0],args[0]).into());
    }

    let client: Client = get_client().await?;

    let db: mongodb::Database = client.database("test_db");
    let collection: mongodb::Collection<_> = db.collection("test_collection");

    let funcao_escolhida = args.get(1).unwrap();

    if funcao_escolhida == "load" {
        let number_of_thread: i32 = args.get(2).unwrap_or(&"10".to_string()).parse::<i32>()?;
        let number_of_insertions: i32 = args.get(3).unwrap_or(&"10".to_string()).parse::<i32>()?;

        pre_load(number_of_thread, number_of_insertions, &collection).await;
        return Ok(())
    }
    
    if funcao_escolhida != "test" {
        return Err("Funcao invalida".into());
    }

    let collection_stats: mongodb::Collection<_> = db.collection("stats_collection");
    
    collection.drop_indexes(None).await?;

    let number_of_insertions: i32 = 10;
    let delay_time: Duration = time::Duration::from_secs(60);

    for test_number in 1..10  {
        for index in 0..64  {

            if index > 0 {
    
                let index_options: IndexModel = IndexModel::builder()
                    .keys(doc! {format!("field{}", index): 1 })
                    .build();
        
                collection.create_index(index_options, None).await?;
    
                 // delay para normaliza o uso de CPU e etc ...
                println!("Aguardando {} segundos para proxima normalizar a CPU", delay_time.as_secs());
                thread::sleep(delay_time);
            }
    
            let result: u128 = insert_documents(number_of_insertions, &collection).await?;
            println!("Insercao de {} documentos com {} indices, demorou - {}ms", number_of_insertions, index, result);
    
            //collection.delete_many(doc! {}, None).await?;
            collection_stats.insert_one(doc! {"docs": number_of_insertions, "indexes": index, "time": result as u32, "testNumber": test_number}, None).await?;
        }

        collection.drop_indexes(None).await?;
        println!("Aguardando {} segundos para o proximo teste", delay_time.as_secs());
        thread::sleep(delay_time);
    }

    Ok(())
}