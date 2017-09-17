package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Substitution, Term}
import fr.lirmm.graphik.graal.core.TreeMapSubstitution
import uk.ac.bbk.dcs.stypes.ConstantType.EPSILON

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Created by
  * Salvatore Rapisarda
  * Stanislav Kikot
  *
  * on 26/04/2017.
  */
case class Type( homomorphism: Substitution ){


  def getVar1: List[Term] =
    homomorphism.getTerms.asScala
      .filter((t: Term) =>
        homomorphism.createImageOf(t).equals( ConstantType.EPSILON)).toList

  def getVar2 (atoms: Vector[Atom]): List[Term] = {
    val anonymousVariables = homomorphism.getTerms.asScala
      .filter((t: Term) =>
        ! homomorphism.createImageOf(t).equals(ConstantType.EPSILON)).toList

     anonymousVariables.flatMap( t => atoms(homomorphism.createImageOf(t).asInstanceOf[ConstantType].getIdentifier._1).getTerms.asScala.toSet )

  }

  def getVar(atoms: List[Atom]):List[Term] = getVar1:::getVar2(atoms.toVector)


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
      Type(this.homomorphism)

   // val genAtoms = this.genAtoms ++ t.genAtoms //  genAtomBuilder.build

    val substitution = new TreeMapSubstitution(homomorphism)
    substitution.put(t.homomorphism)

    Type(substitution)
  }


  /**
    * This method  returns the projection of a give type on to a set of variables
    *
    * @param dest is a set of variables
    * @return a new { @link Type}
    */
  def projection(dest: Set[Term]): Type = {
    val homomorphismProj = new TreeMapSubstitution
    homomorphism.getTerms.asScala.foreach(term => {
      val varialbe = homomorphism.createImageOf(term)
      if (dest.contains(term))
        homomorphismProj.put(term, varialbe)
    })

//    val genAtomProj =
//      genAtoms.filter(entry => dest.contains(entry._1))

    Type( homomorphismProj)
  }

  /**
    * It check that all the terms of the atom are ConstantType EPSILONs
    *
    * @param atom is the atom to check
    * @return true or false
    */
  def  areAllEpsilon(atom:Atom):Boolean =
    visitBagAtoms(atom.getTerms.asScala.toList,
      x =>  ! EPSILON.equals( homomorphism.createImageOf(x).asInstanceOf[Any] ))

  /**
    * It check that all the terms of the atom are ConstantType
    *  Anonymous individuals or ar not epsilon
    *
    *
    * @param atom is the atom to check
    * @return true or false
    */
  def  areAllAnonymous(atom:Atom):Boolean =
    visitBagAtoms(atom.getTerms.asScala.toList,
      (x) => EPSILON.equals( homomorphism.createImageOf(x).asInstanceOf[Any] ) )


  @tailrec
  private def visitBagAtoms(terms: List[Term], f: Term => Boolean  ): Boolean = terms match {
    case List() => true
    case x::xs => if (f(x)) false else visitBagAtoms(xs, f)

  }



  def getFirstAnonymousIndex(atom:Atom): Int ={

    @tailrec
    def visitBagAtoms(terms: List[Term] ): Int = terms match {
      case List() => -1
      case x::xs =>
        if (  EPSILON.equals( homomorphism.createImageOf(x).asInstanceOf[Any] )  )  visitBagAtoms(xs)
        else  homomorphism.createImageOf(x) match  {
          case el:ConstantType => el.getIdentifier._1
          case _ => -1
        }

    }

    visitBagAtoms( atom.getTerms.asScala.toList )

  }



  override def toString: String = {
    s"(homomorphism: $homomorphism)"
  }

}
