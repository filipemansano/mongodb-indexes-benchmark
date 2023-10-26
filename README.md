# MongoDB Benchmarking Application

This application is a benchmarking program that measures the performance of inserting documents into a MongoDB collection. It allows you to evaluate how performance varies with different numbers of indexes in a collection and generates statistics based on the results. The application is written in Rust and uses the `mongodb` library to interact with a MongoDB server.

## Usage

To run the program, follow these steps:

1. Clone this repository to your local machine.
2. Make sure you have Rust installed on your system.
3. Configure the connection URL to your MongoDB cluster in the code (replace `<user>` and `<password>` with the correct URL).
4. Open a terminal in the cloned repository folder.
5. Run the `cargo run` command to start the application.

The program will perform benchmarking of document insertion into a MongoDB collection, varying the number of indexes created in the collection. It will measure the average time required to insert documents based on the specified number of indexes and provide results and statistics.

## Requirements

- Rust (https://www.rust-lang.org/)
- MongoDB (MongoDB server and proper configuration)

## Example Execution

Here is an example of how the application can be run:

```bash
cargo run
```