// generated automatically, do not modify.

package org.ray.api.job;

import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.function.RayFunc0;
import org.ray.api.function.RayFunc1;
import org.ray.api.function.RayFunc2;
import org.ray.api.function.RayFunc3;
import org.ray.api.function.RayFunc4;
import org.ray.api.function.RayFunc5;
import org.ray.api.function.RayFunc6;
import org.ray.api.function.RayFuncVoid0;
import org.ray.api.function.RayFuncVoid1;
import org.ray.api.function.RayFuncVoid2;
import org.ray.api.function.RayFuncVoid3;
import org.ray.api.function.RayFuncVoid4;
import org.ray.api.function.RayFuncVoid5;
import org.ray.api.function.RayFuncVoid6;
import org.ray.api.options.JobOptions;

/**
 * Util class for submitting jobs, with type-safe interfaces. 
 **/
@SuppressWarnings({"rawtypes", "unchecked"})
public interface RayJobs {
  static <R> RayJob<R> submitJob(RayFunc0<R> f, JobOptions options) {
    Object[] args = new Object[]{};
    return Ray.internal().submitJob(f, args, options);
  }
  static RayJob<Void> submitJob(RayFuncVoid0 f, JobOptions options) {
    Object[] args = new Object[]{};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, R> RayJob<R> submitJob(RayFunc1<T0, R> f, T0 t0, JobOptions options) {
    Object[] args = new Object[]{t0};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, R> RayJob<R> submitJob(RayFunc1<T0, R> f, RayObject<T0> t0, JobOptions options) {
    Object[] args = new Object[]{t0};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0> RayJob<Void> submitJob(RayFuncVoid1<T0> f, T0 t0, JobOptions options) {
    Object[] args = new Object[]{t0};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0> RayJob<Void> submitJob(RayFuncVoid1<T0> f, RayObject<T0> t0, JobOptions options) {
    Object[] args = new Object[]{t0};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, R> RayJob<R> submitJob(RayFunc2<T0, T1, R> f, T0 t0, T1 t1, JobOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, R> RayJob<R> submitJob(RayFunc2<T0, T1, R> f, T0 t0, RayObject<T1> t1, JobOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, R> RayJob<R> submitJob(RayFunc2<T0, T1, R> f, RayObject<T0> t0, T1 t1, JobOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, R> RayJob<R> submitJob(RayFunc2<T0, T1, R> f, RayObject<T0> t0, RayObject<T1> t1, JobOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1> RayJob<Void> submitJob(RayFuncVoid2<T0, T1> f, T0 t0, T1 t1, JobOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1> RayJob<Void> submitJob(RayFuncVoid2<T0, T1> f, T0 t0, RayObject<T1> t1, JobOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1> RayJob<Void> submitJob(RayFuncVoid2<T0, T1> f, RayObject<T0> t0, T1 t1, JobOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1> RayJob<Void> submitJob(RayFuncVoid2<T0, T1> f, RayObject<T0> t0, RayObject<T1> t1, JobOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, R> RayJob<R> submitJob(RayFunc3<T0, T1, T2, R> f, T0 t0, T1 t1, T2 t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, R> RayJob<R> submitJob(RayFunc3<T0, T1, T2, R> f, T0 t0, T1 t1, RayObject<T2> t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, R> RayJob<R> submitJob(RayFunc3<T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, T2 t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, R> RayJob<R> submitJob(RayFunc3<T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, R> RayJob<R> submitJob(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, T2 t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, R> RayJob<R> submitJob(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, R> RayJob<R> submitJob(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, R> RayJob<R> submitJob(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2> RayJob<Void> submitJob(RayFuncVoid3<T0, T1, T2> f, T0 t0, T1 t1, T2 t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2> RayJob<Void> submitJob(RayFuncVoid3<T0, T1, T2> f, T0 t0, T1 t1, RayObject<T2> t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2> RayJob<Void> submitJob(RayFuncVoid3<T0, T1, T2> f, T0 t0, RayObject<T1> t1, T2 t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2> RayJob<Void> submitJob(RayFuncVoid3<T0, T1, T2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2> RayJob<Void> submitJob(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, T1 t1, T2 t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2> RayJob<Void> submitJob(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2> RayJob<Void> submitJob(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2> RayJob<Void> submitJob(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, R> RayJob<R> submitJob(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3> RayJob<Void> submitJob(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, R> RayJob<R> submitJob(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4> RayJob<Void> submitJob(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5, R> RayJob<R> submitJob(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
  static <T0, T1, T2, T3, T4, T5> RayJob<Void> submitJob(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, JobOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().submitJob(f, args, options);
  }
}
