package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Substitution, _}
import fr.lirmm.graphik.graal.core.TreeMapSubstitution
import fr.lirmm.graphik.graal.core.atomset.LinkedListAtomSet
import fr.lirmm.graphik.graal.core.factory.ConjunctiveQueryFactory
import fr.lirmm.graphik.graal.homomorphism.StaticHomomorphism

import scala.collection.JavaConverters._

/**
  * Created by rapissal on 24/06/2017.
  *
  */
class TypeExtender(bag: Bag, hom: Substitution, canonicalModels: Array[AtomSet], atomsToBeMapped: List[Atom]) {
// todo:  hom: Substitution must be something like  hom: Substitution[Term, ConstantType]

  type AtomSetWithCanonicalModelIndex = (AtomSet, Int)


  private def getAtomSetWithCanonicalModelIndex(atom: Atom): Option[AtomSetWithCanonicalModelIndex] = {
    val intersection: Set[Term] = getKnownVariables(atom)
    if (intersection.nonEmpty) {
      val term: Option[Term] = intersection.find(p => !hom.createImageOf(p).equals(ConstantType.EPSILON))
      if (term.isDefined) {
        val canonicalModelIndex = hom.createImageOf(term.get).asInstanceOf[ConstantType].getIdentifier._1
        val cm: AtomSet = canonicalModels(hom.createImageOf(term.get).asInstanceOf[ConstantType].getIdentifier._1)
        Some(cm, canonicalModelIndex)
      } else None
    } else None
  }

  private def getKnownVariables(atom: Atom): Set[Term] = {
    val terms: Set[Term] = atom.getTerms.asScala.toSet
    val homTerms = hom.getTerms.asScala.toSet
    terms.intersect(homTerms)
  }

  private def getUnknownVariables(atom: Atom): Set[Term] = {
    val terms: Set[Term] = atom.getTerms.asScala.toSet
    val knownVariables = getKnownVariables(atom)

    if (knownVariables.isEmpty)
      terms
    else
      terms.filter(!knownVariables.contains(_))

  }

  private def isGood(substitution: Substitution, atom: Atom): Boolean = {
    def isBadTerm(term: Term) = {
      val homTerm: Term = hom.createImageOf(term)
      val subTerm = substitution.createImageOf(term)
      (!(subTerm.equals(homTerm) || homTerm.equals(ConstantType.EPSILON))
        || homTerm.equals(ConstantType.EPSILON) && !ReWriter.isAnonymous(subTerm))
    }

    val knownVariables: Set[Term] = getKnownVariables(atom)
    knownVariables.collectFirst { case _ => isBadTerm(_) }.isEmpty

  }

  private def extend( atom: Atom,  answers: List[Substitution], canonicalModelIndex:Int, atomsToBeMapped: List[Atom]): List[TypeExtender] = {
    // atomsToBeMapped

    def extendH( answers: List[Substitution], canonicalModelIndex: Int, acc: List[TypeExtender], atomsToBeMapped:List[Atom] )
    : List[TypeExtender] = answers match {
      case List() => acc
      case head::tail => {
        val substitution= extendSubstitution( atom, head,canonicalModelIndex )
        val t = new TypeExtender( bag, substitution, canonicalModels, atomsToBeMapped  )
        // extendH(tail, canonicalModelIndex, acc :: extendSubstitution( , head))
        acc
      }
    }
    List()
    //extendH(answers, canonicalModel, List())
  }

  private def extendSubstitution (atom:Atom, answer :Substitution, canonicalModelIndex: Int):Substitution ={

    def extendToTheSetOfVariables ( terms:List[Term], modifiedHom: Substitution  ) :Substitution = terms match {
      case List()=>  modifiedHom
      case  head :: tail => extendToTheSetOfVariables( tail,  extendVariable ( head, answer, canonicalModelIndex, modifiedHom ) )
    }

    val modifiedHom: Substitution  = new TreeMapSubstitution(hom)
    val unknownVariables: List[Term] = getKnownVariables( atom).toList
    extendToTheSetOfVariables(unknownVariables , modifiedHom  )

  }


  private def extendVariable(term:Term, answer :Substitution, canonicalModelIndex: Int, modifiedHom: Substitution):Substitution = {
    if ( ! ReWriter.isAnonymous( answer.createImageOf( term) ) ) {
      modifiedHom.put( term, new  ConstantType( (canonicalModelIndex, answer.createImageOf(term).getLabel ) )  )
    }else{
      modifiedHom.put( term, ConstantType.EPSILON )
    }
    modifiedHom
  }



   var children =
     if (atomsToBeMapped.nonEmpty) {
       val currentAtom = atomsToBeMapped.head
       val atomSet = getAtomSetWithCanonicalModelIndex(currentAtom)
       // If the atom set is defined then the atom is connected
       if (atomSet.isDefined) {

         val cq = ConjunctiveQueryFactory.instance.create(new LinkedListAtomSet(currentAtom))
         val result: List[Substitution] = StaticHomomorphism.instance.execute(cq, atomSet.get._1).asScala.toList
         val goodSubstitutions: List[Substitution] = result.filter(isGood(_, currentAtom))
         extend(currentAtom, goodSubstitutions, atomSet.get._2, atomsToBeMapped.tail)

       }
//       else
//         extend(atomsToBeMapped.tail.head, goodSubstitutions, atomSet.get._2, atomsToBeMapped.tail.tail)

     }

}
