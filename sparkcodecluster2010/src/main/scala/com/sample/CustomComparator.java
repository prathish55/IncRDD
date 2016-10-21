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

public  class CustomComparator implements Comparator,Serializable{

        private  final long serialVersionUID = 1L;

        @Override
        public int compare(Object o1, Object o2) {
            String s1 = (String) o1;
            String s2 = (String) o2;
            String[] p1 = StringUtils.splitPreserveAllTokens(s1, ',');
            String[] p2 = StringUtils.splitPreserveAllTokens(s2, ',');
            Integer y1 = Integer.parseInt(p1[0]);
            Integer y2 = Integer.parseInt(p2[0]);
            int result = y1.compareTo(y2);
            if(result==0){
                Integer m1 = Integer.parseInt(p1[1]);
                Integer m2 = Integer.parseInt(p2[1]);
                result = m1.compareTo(m2);
            }            
            if(result==0){
                Integer d1 = Integer.parseInt(p1[2]);
                Integer d2 = Integer.parseInt(p2[2]);
                result = d1.compareTo(d2);
            }            
            return result;
        }
}
