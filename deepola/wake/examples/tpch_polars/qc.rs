// TODO: You need to implement the query c.sql in this file.
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

    let table_columns = HashMap::from([
        
        (
            "customer".into(),
            vec![
                "c_custkey",
                "c_name",
                "c_acctbal",
            ],
        ),
        (
            "orders".into(),
            vec![
                "o_orderkey", 
                "o_custkey", 
                "o_orderdate"
            ],
        ),
        (
            "lineitem".into(),
            vec![
                "l_orderkey",
                "l_extendedprice",
                "l_discount",
            ],
        ),
        
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let customer_csvreader_node = build_csv_reader_node("customer".into(), &tableinput, &table_columns);

    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("o_orderdate").unwrap();
            let mask = a.gt_eq("1993-10-01").unwrap() & a.lt("1994-01-01").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let hash_join_node_1 = HashJoinBuilder::new()
        .left_on(vec!["c_custkey".into()])
        .right_on(vec!["o_custkey".into()])
        .build();

    let hash_join_node_2 = HashJoinBuilder::new()
        .left_on(vec!["l_orderkey".into()])
        .right_on(vec!["o_orderkey".into()])
        .build();

    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let discount = Series::new(
                "discount",
                extended_price
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * (discount * -1f64 + 1f64),
            );
            df.hstack(&[discount]).unwrap()
        })))
        .build();

    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec![
            "c_custkey".into(),
            "c_name".into(),
            "c_acctbal".into(),
        ])
        .set_aggregates(vec![("discount".into(), vec!["sum".into()])]);
    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let cols = vec![
                Series::new("c_custkey", df.column("c_custkey").unwrap()),
                Series::new("c_name", df.column("c_name").unwrap()),
                Series::new("c_acctbal", df.column("c_acctbal").unwrap()),
                Series::new("revenue", df.column("discount_sum").unwrap()),
            ];
            DataFrame::new(cols)
                .unwrap()
                .sort(&["revenue"], vec![true])
                .unwrap()
        })))
        .build();


    where_node.subscribe_to_node(&orders_csvreader_node, 0);
    hash_join_node_1.subscribe_to_node(&customer_csvreader_node, 0);
    hash_join_node_1.subscribe_to_node(&where_node, 1);
    hash_join_node_2.subscribe_to_node(&lineitem_csvreader_node, 0);
    hash_join_node_2.subscribe_to_node(&hash_join_node_1, 1);
    expression_node.subscribe_to_node(&hash_join_node_2, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(customer_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(where_node);
    service.add(hash_join_node_1);
    service.add(hash_join_node_2);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}