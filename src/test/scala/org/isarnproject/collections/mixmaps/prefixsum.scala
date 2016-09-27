/*
Copyright 2016 Erik Erlandson

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.isarnproject.collections.mixmaps.prefixsum

import scala.collection.SortedMap

import org.scalatest._

import com.twitter.algebird.{ Monoid, Aggregator, MonoidAggregator }

import org.isarnproject.scalatest.matchers.seq._
import org.isarnproject.algebirdAlgebraAPI.implicits._

object PrefixSumMapProperties extends FlatSpec with Matchers {
  import tree._
  import infra._

  // Assumes 'data' is in key order
  def testPrefix[K, V, P, IN <: INodePS[K, V, P], M <: PrefixSumMapLike[K, V, P, IN, M] with SortedMap[K, V]](
    data: Seq[(K, V)],
    psmap: PrefixSumMapLike[K, V, P, IN, M] with SortedMap[K, V]) {

    val agg = psmap.prefixAggregator
    val psTruth = data.map(_._2).scanLeft(agg.monoid.empty)((v, e) => agg.lff(v, e))
    psmap.prefixSums() should beEqSeq(psTruth.tail)
    psmap.prefixSums(open = true) should beEqSeq(psTruth.dropRight(1))
    psmap.prefixSums() should beEqSeq(psmap.keys.map(k => psmap.prefixSum(k)))
    psmap.prefixSums(open = true) should beEqSeq(psmap.keys.map(k => psmap.prefixSum(k, open = true)))
  }
}

class PrefixSumMapSpec extends FlatSpec with Matchers {
  import org.isarnproject.collections.mixmaps.ordered.RBProperties._
  import org.isarnproject.collections.mixmaps.ordered.OrderedMapProperties._

  import PrefixSumMapProperties._

  def mapType1 =
    PrefixSumMap.key[Int].value[Int]
      .prefix(Aggregator.appendMonoid((ps: Int, v: Int) => ps + v))

  it should "pass randomized tree patterns" in {
    val data = Vector.tabulate(50)(j => (j, j))
    (1 to 1000).foreach { u =>
      val shuffled = scala.util.Random.shuffle(data)
      val psmap = shuffled.foldLeft(mapType1)((m, e) => m + e)

      testRB(psmap)
      testKV(data, psmap)
      testDel(data, psmap)
      testEq(data, mapType1)
      testPrefix(data, psmap)
    }
  }

  it should "serialize and deserialize" in {
    import org.isarnproject.scalatest.serde.roundTripSerDe

    val data = Vector.tabulate(50)(j => (j, j))
    val omap = data.foldLeft(mapType1)((m, e) => m + e)
    val imap = roundTripSerDe(omap)

    (imap == omap) should be (true)
    testRB(imap)
    testKV(data, imap)
    testDel(data, imap)
    testPrefix(data, imap)
  }
}
