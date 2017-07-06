package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Substitution, _}
import fr.lirmm.graphik.graal.core.TreeMapSubstitution
import fr.lirmm.graphik.graal.core.atomset.LinkedListAtomSet
import fr.lirmm.graphik.graal.core.factory.ConjunctiveQueryFactory
import fr.lirmm.graphik.graal.homomorphism.StaticHomomorphism

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Created by
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  * on 24/06/2017.
  *
  */
class TypeExtender(bag: Bag, hom: Substitution, canonicalModels: Array[AtomSet],
                   atomsToBeMapped: List[Atom], variableToBeMapped:List[Term] ) {

  var children: List[TypeExtender] =  getTypesExtension

  def this(bag: Bag, hom: Substitution, canonicalModels: Array[AtomSet] ) =
      this(bag, hom, canonicalModels, bag.atoms.toList, bag.variables.filter( p=> ! hom.getTerms.contains(p) ).toList )



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
      (!(subTerm.getLabel.equals(homTerm.getLabel) || homTerm.equals(ConstantType.EPSILON))
        || homTerm.equals(ConstantType.EPSILON) && !ReWriter.isAnonymous(subTerm))
    }

    val knownVariables: Set[Term] = getKnownVariables(atom)
    val collection = knownVariables.filter(isBadTerm)
    collection.isEmpty
  }

  private def extend( atom: Atom,  answers: List[Substitution], canonicalModelIndex:Int, atoms: List[Atom] ): List[TypeExtender] = {
     @tailrec
    def extendH( answers: List[Substitution], acc: List[TypeExtender]): List[TypeExtender] = answers match {
      case List() => acc
      case x::xs =>
        val substitution= extendSubstitution( atom, x,canonicalModelIndex )
        val unknownVariables=  getUnknownVariables(atom)
        val t = new TypeExtender( bag, substitution, canonicalModels, atoms, variableToBeMapped.filter( v => ! unknownVariables.contains(v) ) )
        extendH(xs, t::acc)

    }
    extendH(answers, List())
  }

  private def extendSubstitution (atom:Atom, answer :Substitution, canonicalModelIndex: Int):Substitution ={

    def extendVariable(term:Term, answer :Substitution, canonicalModelIndex: Int, modifiedHom: Substitution):Substitution = {
      if (ReWriter.isAnonymous( answer.createImageOf( term) ) ) {
        modifiedHom.put( term, new  ConstantType( (canonicalModelIndex, answer.createImageOf(term).getLabel ) )  )
      }else{
        modifiedHom.put( term, ConstantType.EPSILON )
      }
      modifiedHom
    }

    @tailrec
    def extendToTheSetOfVariables ( terms:List[Term], modifiedHom: Substitution  ) :Substitution = terms match {
      case List() =>  modifiedHom
      case  x :: xs => extendToTheSetOfVariables( xs,  extendVariable ( x, answer, canonicalModelIndex, modifiedHom ) )
    }


    val modifiedHom: Substitution  = new TreeMapSubstitution(hom)
    val unknownVariables: List[Term] = getUnknownVariables(atom).toList
    extendToTheSetOfVariables(unknownVariables , modifiedHom  )

  }

  private def  getTypesExtension: List[TypeExtender] =   {

    @tailrec
    def getPossibleConnectedTypesExtensions(atoms: List[Atom], acc: (Boolean,  List[TypeExtender] ) ) :
    ( Boolean,List[TypeExtender]) = atoms match {
      case List() => acc
      case x :: xs  =>
        // getting the tuple of atomSet and CanonicalModelIndex
        val atomSetAndCanModIndex: Option[AtomSetWithCanonicalModelIndex] = getAtomSetWithCanonicalModelIndex(x)
        // If the atom set is defined then the atom is connected
        if (atomSetAndCanModIndex.isDefined) {
          val cq = ConjunctiveQueryFactory.instance.create(new LinkedListAtomSet(x))
          val result: List[Substitution] = StaticHomomorphism.instance.execute(cq, atomSetAndCanModIndex.get._1).asScala.toList
          val goodSubstitutions: List[Substitution] = result.filter( s => isGood( s, x))
          val extension = extend(x, goodSubstitutions, atomSetAndCanModIndex.get._2, atomsToBeMapped.tail)
          getPossibleConnectedTypesExtensions(xs,  ( true, acc._2:::extension))
        }else
          getPossibleConnectedTypesExtensions(xs, acc)

    }

    def extendToATerm( variable:Term ) : List[TypeExtender] = {
      //
      def extend( canonicalModelIndex: Int, maxIndex:Int,  acc:List[TypeExtender] ) : List[TypeExtender] = {
        //
        def extenderInsideM( terms:List[Term], acc:List[TypeExtender]  ) :List[TypeExtender]  =  terms match {
          case List() => acc
          case head::tail =>
            if ( ReWriter.isAnonymous(head) ) extenderInsideM(tail, buildExtender( new ConstantType(canonicalModelIndex,  head.getLabel))::acc)
            else extenderInsideM(tail, acc )
        }

        //
        if  ( maxIndex == canonicalModelIndex)  acc
        else {
          val extendedTerms =   extenderInsideM( canonicalModels(canonicalModelIndex).getTerms.asScala.toList, List() )
          extend(canonicalModelIndex + 1, maxIndex, extendedTerms:::acc  )
        }
      }

      def buildExtender( term: ConstantType ) : TypeExtender = {
        val h = new TreeMapSubstitution(hom)
        h.put(variable, term)
        new TypeExtender(bag, h, canonicalModels, atomsToBeMapped, variableToBeMapped.tail )
      }

      extend( 0, canonicalModels.length,  List(buildExtender(ConstantType.EPSILON)))
    }

    def filterThroughAtoms(atoms:List[Atom]):  List[TypeExtender] ={
      List()
    }

    val possibleConnectedExtensions = getPossibleConnectedTypesExtensions(atomsToBeMapped, (false, List()) )
    if ( possibleConnectedExtensions._1 )
      possibleConnectedExtensions._2
    else if (variableToBeMapped.isEmpty)
      filterThroughAtoms(atomsToBeMapped)
    else
      extendToATerm( variableToBeMapped(0) )


  }




}
