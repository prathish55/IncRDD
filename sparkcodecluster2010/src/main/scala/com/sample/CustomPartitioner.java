package com.sample;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public  class CustomPartitioner extends Partitioner implements Serializable{
        private  final long serialVersionUID = 1L;
        private int partitions;
        public CustomPartitioner(int noOfPartitioners){
            partitions=noOfPartitioners; 
        }
        @Override
        public int getPartition(Object key) {
            String[] sa = StringUtils.splitPreserveAllTokens(key.toString(), ',');
            int y = (Integer.parseInt(sa[0])-1987);
            return (y%partitions);
        }

        @Override
        public int numPartitions() {
            return partitions;
        }        
}
