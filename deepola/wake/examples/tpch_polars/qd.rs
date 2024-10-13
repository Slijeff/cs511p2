// TODO: You need to implement the query d.sql in this file.

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
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec![
                "l_extendedprice", 
                "l_discount", 
                "l_partkey",
                "l_quantity", 
                "l_shipinstruct"
            ],
        ),
        (
            "part".into(), 
            vec![
            "p_partkey", 
            "p_brand", 
            "p_size"
        ]),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
    .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        let l_shipinstruct = df.column("l_shipinstruct").unwrap();
        let l_quantity = df.column("l_quantity").unwrap();
        let p_brand = df.column("p_brand").unwrap();
        let p_size = df.column("p_size").unwrap();
        let mask = l_shipinstruct.equal("DELIVER IN PERSON").unwrap() 
        & 
        (
            (p_brand.equal("Brand#12").unwrap() & l_quantity.gt_eq(1).unwrap() & l_quantity.lt_eq(11).unwrap() & p_size.gt_eq(1).unwrap() & p_size.lt_eq(5).unwrap()) |
            (p_brand.equal("Brand#23").unwrap() & l_quantity.gt_eq(10).unwrap() & l_quantity.lt_eq(20).unwrap() & p_size.gt_eq(1).unwrap() & p_size.lt_eq(10).unwrap()) |
            (p_brand.equal("Brand#34").unwrap() & l_quantity.gt_eq(20).unwrap() & l_quantity.lt_eq(30).unwrap() & p_size.gt_eq(1).unwrap() & p_size.lt_eq(15).unwrap())
        )
        ;
        let result = df.filter(&mask).unwrap();
        result
    })))
    .build();

    let hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
    .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        let l_extendedprice = df.column("l_extendedprice").unwrap();
        let l_discount = df.column("l_discount").unwrap();
        let columns = vec![
            Series::new(
                "revenue",
                l_extendedprice
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * (l_discount * -1f64 + 1f64),
            ),
        ];
        df.hstack(&columns).unwrap()
    })))
    .build();


    let sum_accumulator = SumAccumulator::new();
    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();


    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
    .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        let columns = vec![
            Series::new("revenue", df.column("revenue").unwrap()),
        ];
        DataFrame::new(columns).unwrap()
    })))
    .build();

    hash_join_node.subscribe_to_node(&lineitem_csvreader_node, 0); 
    hash_join_node.subscribe_to_node(&part_csvreader_node, 1);
    where_node.subscribe_to_node(&hash_join_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);
    output_reader.subscribe_to_node(&select_node, 0);
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(where_node);
    service.add(part_csvreader_node);
    service.add(hash_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service

}
