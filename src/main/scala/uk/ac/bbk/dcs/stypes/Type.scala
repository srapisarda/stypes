package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Substitution, Term, Variable}
import fr.lirmm.graphik.graal.core.TreeMapSubstitution
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Created by
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  *   on 26/04/2017.
  */
case class Type(genAtoms: Map[Term, Atom], homomorphism: Substitution) {

   def getVar1: List[Term] =
    homomorphism.getTerms.asScala
      .filter((t: Term) =>
        homomorphism.createImageOf(t).getLabel.startsWith("epsilon")).toList

  def getVar2:List[Term] = {

    @tailrec
    def visitAtomsTems (atoms:List[Atom], acc:List[Variable]   ) : List[Variable]  =  atoms match {
      case List() => acc
      case x::xs => visitAtomsTems( xs, acc ::: getTerms(x.getTerms.asScala.toList, List()) )
    }

    @tailrec
    def getTerms ( terms: List[Term], acc:List[Variable] ) : List[Variable]  = terms match  {
      case List() => acc.reverse
      case x::xs => getTerms( xs, DefaultTermFactory.instance.createVariable("v" + x.getLabel) :: acc)
    }

    val ee = homomorphism.getTerms.asScala
      .filter((t: Term) =>
        homomorphism.createImageOf(t).getLabel.startsWith("EE")).toList

    if (ee.nonEmpty)
      visitAtomsTems( genAtoms.values.toSet.toList, List())
    else
      List()

  }


  /**
    * This method returns the type domain
    *
    * @return a set of { @link Term}s which is the domain
    */
  def getDomain: Set[Term] =
    homomorphism.getTerms.asScala.toSet


  /**
    * This method makes a union of the self object and the type
    * We assume that the types are compatible,
    * which it means that they are coincide on their common domain
    *
    * @param t is a { @link Type}
    * @return a { @link Type}
    */
  def union(t: Type): Type = {
    if (t == null)
        Type(this.genAtoms, this.homomorphism)

    val genAtoms =  this.genAtoms  ++   t.genAtoms //  genAtomBuilder.build

    val substitution = new TreeMapSubstitution(homomorphism)
    substitution.put(t.homomorphism)

    Type(genAtoms, substitution)
  }


  /**
    * This method  returns the projection of a give type on to a set of variables
    *
    * @param dest is a set of variables
    * @return a new { @link Type}
    */
  def projection(dest: Set[Term]): Type = {
    val homomorphismProj = new TreeMapSubstitution
    homomorphism.getTerms.asScala.foreach( term => {
      val varialbe =  homomorphism.createImageOf(term)
      if (dest.contains(term))
        homomorphismProj.put(term, varialbe)
    })

    val genAtomProj =
      genAtoms.filter( entry => dest.contains(entry._1))

    Type(genAtomProj, homomorphismProj)
  }


  override def toString: String = {
    s"atoms: $genAtoms, homomorphism: $homomorphism, var1: $getVar1, var2: $getVar2"
  }

}
