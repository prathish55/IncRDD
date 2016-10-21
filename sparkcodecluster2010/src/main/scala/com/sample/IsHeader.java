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

    public class IsHeader implements Function<String,Boolean>{
        @Override
        public Boolean call(String l) throws Exception {
            return !(l.startsWith("Year"));        
        }        
    }
