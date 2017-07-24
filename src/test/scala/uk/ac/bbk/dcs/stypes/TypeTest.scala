package uk.ac.bbk.dcs.stypes

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

  // todo: this test should be reviewed using ConstantType instead of using string (for example "epsilon")
  def setUp(): Unit = {
    val s1 = new TreeMapSubstitution
    tx = DefaultTermFactory.instance.createVariable("X")
    val ee0 =  DefaultTermFactory.instance.createConstant("EE0")
    s1.put(tx, ee0)


    ty = DefaultTermFactory.instance.createVariable( "Y")
    val epsilon = DefaultTermFactory.instance.createConstant("epsilon")
    s1.put(ty, epsilon)
    val atom1 = new DefaultAtom(new Predicate("A", 1))
    atom1.setTerm(0, DefaultTermFactory.instance.createConstant("a0"))


    //type1
    type1 = Type(s1)


    // type2
    val s2 = new TreeMapSubstitution
    s2.put(ty, epsilon)
    val tz = DefaultTermFactory.instance.createVariable("Z")
    s2.put(tz, ee0)
    val atom2 = new DefaultAtom(new Predicate("B", 1))
    atom2.setTerm(0, DefaultTermFactory.instance.createConstant("a0"))
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

    it("should ensure that areAllAnonymous is working "){


    }


  }





}
