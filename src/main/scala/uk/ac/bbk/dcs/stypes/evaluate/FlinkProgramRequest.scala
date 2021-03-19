package uk.ac.bbk.dcs.stypes.evaluate

import fr.lirmm.graphik.graal.api.core.Atom
import uk.ac.bbk.dcs.stypes.Clause

case class FlinkProgramRequest(datalog: List[Clause], edbMap: Map[Atom, EdbProperty],
                               properties: FlinkProgramProperties )