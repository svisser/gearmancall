extern crate byteorder;
extern crate rand;
extern crate rustc_serialize;

use std::env;
use std::io::prelude::*;
use std::net::TcpStream;
use std::time::Duration;

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use rand::{thread_rng, Rng};
use rustc_serialize::json::Json;

const HELP: &'static str = "
Command-line tool for calling Gearman

Usage:
    gearmancall <function> <function arguments in JSON>

Example:
    gearmancall get_handler {\\\"handler_id\\\":123}

Options:
    -h, --help           Display this message
";

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 || args[1] == "-h" || args[1] == "--help" {
        println!("{}", HELP);
        return
    }

    let endpoint: String = args[1].clone();

    {
        let mut stream = TcpStream::connect("127.0.0.1:4730").expect("Couldn't connect to server");
        stream.set_read_timeout(Some(Duration::from_millis(10000))).expect("set_read_timeout failed");

        let job_id: String = thread_rng().gen_ascii_chars().take(16).collect();
        let job_arguments: String = args[2].to_string();

        println!("Calling Gearman with function '{}'", endpoint);
        println!("{}", job_arguments);

        let mut submit_job_data: Vec<u8> = Vec::new();
        submit_job_data.extend(endpoint.into_bytes());
        submit_job_data.extend(b"\0");
        submit_job_data.extend(job_id.into_bytes());
        submit_job_data.extend(b"\0");
        submit_job_data.extend(job_arguments.into_bytes());

        let data_length: usize = submit_job_data.len();
        if data_length > 4294967296 {
            panic!("Gearman cannot handle jobs larger than 2^32");
        }

        let mut request: Vec<u8> = Vec::new();
        request.extend(b"\0REQ");
        request.write_u32::<BigEndian>(7).unwrap();  // SUBMIT_JOB
        request.write_u32::<BigEndian>(data_length as u32).unwrap();
        request.extend(submit_job_data);

        stream.write_all(&request).unwrap();

        loop {
            let mut raw_response = [0; 10000];

            match stream.read(&mut raw_response) {
                Err(_) => {
                    println!("Did not receive a response from Gearman. Exiting.");
                    break;
                }
                Ok(_) => {
                    let packet_type: u32 = BigEndian::read_u32(&raw_response[4 .. 8]);
                    let response_size: u32 = BigEndian::read_u32(&raw_response[8 .. 12]);

                    if packet_type == 13 {
                        let start = 30 as usize;
                        let end = 12 + response_size as usize;

                        let response = String::from_utf8_lossy(&raw_response[start .. end]);

                        match Json::from_str(&response) {
                            Ok(json_response) => println!("Result: {}", json_response.pretty()),
                            Err(_) => println!("Unable to parse response as JSON {:?}", response),
                        }
                        break;
                    }
                }
            }
        }
    }
}
