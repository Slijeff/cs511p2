// TODO: You need to implement the query a.sql in this file.
use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([(
        "part".into(),
        vec![
            "p_retailprice",
            "p_size",
            "p_container",
            "p_mfgr"
        ],
    )]);

    // CSVReaderNode would be created for this table.
    let part_csvreader_node =
        build_csv_reader_node("part".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_retailprice = df.column("p_retailprice").unwrap();
            let p_container = df.column("p_container").unwrap();
            let p_size = df.column("p_size").unwrap();
            let p_mfgr = df.column("p_mfgr").unwrap();
            let mask = p_retailprice.gt(1000).unwrap() &
                        p_container.not_equal("JUMBO JAR").unwrap() &
                        (p_size.equal(5).unwrap() | 
                        p_size.equal(10).unwrap() | 
                        p_size.equal(15).unwrap() | 
                        p_size.equal(20).unwrap()) & 
                        p_mfgr.equal("Manufacturer#1").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();


    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
    .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        let p_retailprice = df.column("p_retailprice").unwrap();
        let columns = vec![
            Series::new(
                "total",
                p_retailprice
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
            ),
        ];
        df.hstack(&columns).unwrap()
    })))
    .build();

    // AGGREGATE Node
    let sum_accumulator = SumAccumulator::new();
    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let columns = vec![
                Series::new("total", df.column("total").unwrap()),
            ];
        DataFrame::new(columns).unwrap()
        })))
        .build();



    // Connect nodes with subscription
    where_node.subscribe_to_node(&part_csvreader_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(select_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(part_csvreader_node);
    service

}
