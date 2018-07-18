package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * stypes
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


  private def createDependentType (newSplitter:Splitter, borderType:Type, splittingTyper:Type):Type = {
    val typeUnion = borderType.union(splittingType)

    val borderTerms = typeUnion.getDomain.intersect( newSplitter.getAllTerms )

    typeUnion.projection( borderTerms )

  }

  def generateChildPredicate(currentSplitter: Splitter): Atom ={
    val up = createDependentType(currentSplitter, borderType, splittingType)

    val terms:List[Term] = up.getVar(generatingAtoms)

    val predicate = getNewPredicate ( (currentSplitter, up), terms.size )

    new DefaultAtom( predicate, terms.asJava)

  }
  
  def GetAllSubordinateRules:List[RuleTemplate] = splitter.children.flatMap(c =>
    reWriter.generateRewriting(createDependentType(c,borderType,splittingType), c))
  
  
  
}

