package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * stype
 * %%
 * Copyright (C) 2017 - 2021 Birkbeck University of London
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import fr.lirmm.graphik.graal.api.core._
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.core.{DefaultAtom, TreeMapSubstitution}
import org.scalatest.{BeforeAndAfter, FunSpec}

/**
  * Created by
  *   Salvatore Rapisarda on 26/04/2017.
  */
class TypeTest extends FunSpec with BeforeAndAfter {
  private var type1:Type= _
  private var type2:Type = _

  private var tx:Variable = _
  private var ty:Variable = _

  private var  atom1:Atom =_
  private var atom2:Atom = _
  // todo: this test should be reviewed using ConstantType instead of using string (for example "epsilon")
  def setUp(): Unit = {
    val s1 = new TreeMapSubstitution
    tx = DefaultTermFactory.instance.createVariable("X")
    s1.put(tx, ConstantType.CS0)


    ty = DefaultTermFactory.instance.createVariable( "Y")
    s1.put(ty, ConstantType.EPSILON)
    atom1 = new DefaultAtom(new Predicate("A", 1))
    atom1.setTerm(0, DefaultTermFactory.instance.createVariable("U"))


    //type1
    type1 = Type(s1)


    // type2
    val s2 = new TreeMapSubstitution
    s2.put(ty, ConstantType.EPSILON)
    val tz = DefaultTermFactory.instance.createVariable("Z")
    s2.put(tz, ConstantType.CS1)
    atom2 = new DefaultAtom(new Predicate("B", 2) )
    atom2.setTerm(0, DefaultTermFactory.instance.createVariable("U"))
    atom2.setTerm(1, DefaultTermFactory.instance.createVariable("V"))
    type2 = Type(s2)


  }


  before{
    setUp()
  }

  describe("The Typse basic test") {
    it("should have a domain contained 3 terms after union"){
//      println("union test")
//      println(s"type1:  $type1")
//      println(s"type2:  $type2")

      val actual = type1.union(type2)
//      println(s"union: type1 U type2: $actual")
      assert(actual.getVar1.size==1)
     //  assert(actual.getVar2().size==2)
      assert(actual.getDomain.size == 3)

    }

    it ("should have a domain of 2 terms after projection "){
//      println("projection test")
//      println("type1: " + type1)
//      println("type2: " + type2)

      val actual = type1.union(type2).projection(Set(tx, ty))
      assert(actual.getVar1.size==1)
      assert(actual.getVar1.size==1)
//      println("projection : proj(x, y) from (type1 U type2): " + actual)

      assert(actual.getDomain.size == 2)
    }

    it("should get var2"){
      val res = type1.getVar2(Vector(atom1, atom2))
      assert(res.head.getLabel.equals("U"))

      val res2 = type2.getVar2(Vector(atom1, atom2))
      assert(res2.size==2)
      assert(res2.head.getLabel.equals("U"))
      assert(res2.tail.head.getLabel.equals("V"))
    }


  }





}
