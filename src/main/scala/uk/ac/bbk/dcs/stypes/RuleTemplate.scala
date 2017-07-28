package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import scala.collection.JavaConverters._

/**
  * Created by
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  * on 25/07/2017.
  */
class RuleTemplate (splitter: Splitter, borderType:Type,  splittingType:Type, generatingAtoms:List [Atom], reWriter: ReWriter) {

  type RulePredicate = (Splitter, Type)

  val terms:List[Term] = borderType.getVar(generatingAtoms)

  def getNewPredicate ( rp: RulePredicate, arity:Int ) : Predicate = new Predicate( rp , arity  )

  val head:Atom = new DefaultAtom(getNewPredicate((splitter, borderType), terms.size) , terms.asJava)

  val body:List[Any] =
    reWriter.makeAtoms( splitter.getSplittingVertex , splittingType) ::: splitter.children.map(generateChildPredicate)


  def generateChildPredicate(currentSplitter: Splitter): Atom ={
    val typeUnion = borderType.union(splittingType)

    val borderTerms = typeUnion.getDomain.intersect( currentSplitter.getAllTerms )

    val up: Type =  typeUnion.projection( borderTerms )

    val terms:List[Term] = up.getVar(generatingAtoms)

    val predicate = getNewPredicate ( (currentSplitter, up), terms.size )

    new DefaultAtom( predicate, terms.asJava)

  }
  
  def GetAllSubordinateRules:List[RuleTemplate] = splitter.children.map(c => reWriter.generateRewriting(up, c)).flatten
  
  
  
}

