package org.qcri.rheem.apps.util

import de.hpi.isg.profiledb.store.model.Experiment
import org.junit.{Assert, Test}

/**
  * Test suite for the [[Parameters]].
  */
class ParametersTest {

  @Test
  def testCreateExperimentFull(): Unit = {
    val experiment = Parameters.createExperiment("exp(id;tags=tag1,tag2;conf=a:1,b:true)", TestExperimentDescriptor)

    val expectation = new Experiment("id", TestExperimentDescriptor.createSubject, experiment.getStartTime, "tag1", "tag2")
    expectation.getSubject.addConfiguration("a", 1)
    expectation.getSubject.addConfiguration("b", true)

    Assert.assertEquals(expectation, experiment)
  }

  @Test
  def testCreateExperimentMinimal(): Unit = {
    val experiment = Parameters.createExperiment("exp(id)", TestExperimentDescriptor)

    val expectation = new Experiment("id", TestExperimentDescriptor.createSubject, experiment.getStartTime)

    Assert.assertEquals(expectation, experiment)
  }

  @Test
  def testCreateExperimentWithTags(): Unit = {
    val experiment = Parameters.createExperiment("exp(id;tags=tag1,tag2)", TestExperimentDescriptor)

    val expectation = new Experiment("id", TestExperimentDescriptor.createSubject, experiment.getStartTime, "tag1", "tag2")

    Assert.assertEquals(expectation, experiment)
  }

  @Test
  def testCreateExperimentWithConf(): Unit = {
    val experiment = Parameters.createExperiment("exp(id;conf=myLong:-234L,myDouble:-23.42)", TestExperimentDescriptor)

    val expectation = new Experiment("id", TestExperimentDescriptor.createSubject, experiment.getStartTime)
    expectation.getSubject.addConfiguration("myLong", -234L)
    expectation.getSubject.addConfiguration("myDouble", -23.42d)

    Assert.assertEquals(expectation, experiment)
  }

  @Test
  def testParseAny(): Unit = {
    Assert.assertEquals(-234L, Parameters.parseAny("-234L"))
    Assert.assertEquals(-234, Parameters.parseAny("-234"))
    Assert.assertEquals(0, Parameters.parseAny("0"))
    Assert.assertEquals(23d, Parameters.parseAny("23."))
    Assert.assertEquals(0d, Parameters.parseAny("0."))
    Assert.assertEquals(-32.23d, Parameters.parseAny("-32.23"))
    Assert.assertEquals(true, Parameters.parseAny("true"))
    Assert.assertEquals(false, Parameters.parseAny("false"))
    Assert.assertEquals(null, Parameters.parseAny("null"))
    Assert.assertEquals("astring", Parameters.parseAny("astring"))
  }


  object TestExperimentDescriptor extends ExperimentDescriptor {

    override def version: String = "1.0"
  }
}
