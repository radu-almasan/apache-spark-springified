package com.radualmasan.blog.apache_spark_springified.function;

@FunctionalInterface
public interface UncheckedConsumer<T> {

    void accept(T t) throws Exception;

}
