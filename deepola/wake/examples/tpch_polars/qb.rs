// TODO: You need to implement the query b.sql in this file.
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
            "supplier".into(),
            vec!["s_suppkey", "s_nationkey", "s_name"],
        ),
        (
            "orders".into(), 
            vec!["o_totalprice", "o_custkey"],
        ),
        (
            "nation".into(), 
            vec!["n_nationkey", "n_regionkey"],
        ),
        (
            "region".into(), 
            vec!["r_regionkey", "r_name"],
        ),
    ]);

    // CSVReaderNode would be created for this table.
    let supplier_csvreader_node = build_csv_reader_node("supplier".into(), &tableinput, &table_columns);
    let orders_csvreader_node =
        build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_csv_reader_node("nation".into(), &tableinput, &table_columns);
    let region_csvreader_node = build_csv_reader_node("region".into(), &tableinput, &table_columns);

    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let r_name = df.column("r_name").unwrap();
            let mask = r_name.equal("EUROPE").unwrap();
            let result = df.filter(&mask).unwrap();
            result
        })))
        .build();

    let hash_join_node_1 = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();
    let hash_join_node_2 = HashJoinBuilder::new()
        .left_on(vec!["n_regionkey".into()])
        .right_on(vec!["r_regionkey".into()])
        .build();
    let hash_join_node_3 = HashJoinBuilder::new()
        .left_on(vec!["s_suppkey".into()])
        .right_on(vec!["o_custkey".into()])
        .build();

    // GROUP BY Aggregate Node
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["s_name".to_string()])
        .set_aggregates(vec![
            ("o_totalprice".into(), vec!["sum".into()]),
        ]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
    .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        let columns = vec![
            Series::new("s_name", df.column("s_name").unwrap()),
            Series::new("total_order_value", df.column("o_totalprice_sum").unwrap()),
        ];
        DataFrame::new(columns)
            .unwrap()
            .sort(&["total_order_value"], vec![true])
            .unwrap()
    })))
    .build();

    hash_join_node_1.subscribe_to_node(&supplier_csvreader_node, 0); 
    hash_join_node_1.subscribe_to_node(&nation_csvreader_node, 1);
    
    hash_join_node_2.subscribe_to_node(&hash_join_node_1, 0); 
    hash_join_node_2.subscribe_to_node(&region_csvreader_node, 1); 

    hash_join_node_3.subscribe_to_node(&hash_join_node_2, 0);
    hash_join_node_3.subscribe_to_node(&orders_csvreader_node, 1); 

    where_node.subscribe_to_node(&hash_join_node_3, 0);

    groupby_node.subscribe_to_node(&where_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(orders_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(region_csvreader_node);
    service.add(where_node);
    service.add(hash_join_node_1);
    service.add(hash_join_node_2);
    service.add(hash_join_node_3);
    service.add(groupby_node);
    service.add(select_node);
    service

    }

    

