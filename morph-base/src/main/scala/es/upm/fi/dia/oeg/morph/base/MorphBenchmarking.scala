package es.upm.fi.dia.oeg.morph.base

import java.io._

class MorphBenchmarking (benchmark_file_path:String) {
  // Times for the different OBDA phases (according to publication: The NPD Benchmark for OBDA Systems)
  // These times must be in miliseconds
  private var starting_time:Long = -1
  private var rewriting_time:Long = -1
  private var translation_time:Long = -1
  private var execution_time:Long = -1

  private var materialization_time:Long = -1

  //When each of the OBDA phases start, current_phase_start_time is updated
  private var current_phase_start_time:Long = System.currentTimeMillis()


  def finishStartingPhase() = {
    val current_time_millis = System.currentTimeMillis()
    this.starting_time = current_time_millis - this.current_phase_start_time
    //update the current_phase_start_time of the next OBDA phase
    this.current_phase_start_time = current_time_millis
  }

  def finishRewritingPhase() = {
    val current_time_millis = System.currentTimeMillis()
    this.rewriting_time = current_time_millis - this.current_phase_start_time
    //update the current_phase_start_time of the next OBDA phase
    this.current_phase_start_time = current_time_millis
  }

  def finishTranslationPhase() = {
    val current_time_millis = System.currentTimeMillis()
    this.translation_time = current_time_millis - this.current_phase_start_time
    //update the current_phase_start_time of the next OBDA phase
    this.current_phase_start_time = current_time_millis
  }

  def finishExecutionPhase() = {
    //no more OBDA phases to benchmark, current_phase_start_time not updated
    this.execution_time = System.currentTimeMillis() - this.current_phase_start_time

    storeBenchmarkResults()
  }

  def finishMaterializationPhase() = {
    //no more OBDA phases to benchmark, current_phase_start_time not updated
    this.materialization_time = System.currentTimeMillis() - this.current_phase_start_time

    storeBenchmarkResults()
  }

  private def storeBenchmarkResults(): Unit = {
    //build a json string with the benchmark results
    var benchmark_result: String = "{"

    //add to the results those phases that have been benchmarked
    if (starting_time != -1) benchmark_result = benchmark_result + "Starting:" + starting_time.toString() + ","
    //do not include rewriting, as Morph does not include this phase
    //if (rewriting_time != -1) benchmark_result = benchmark_result + "Rewriting:" + rewriting_time.toString() + ","
    if (translation_time != -1) benchmark_result = benchmark_result + "Translation:" + translation_time.toString() + ","
    if (execution_time != -1) benchmark_result = benchmark_result + "Execution:" + execution_time.toString() + ","
    if (materialization_time != -1) benchmark_result = benchmark_result + "Materialization:" + materialization_time.toString() + ","

    //finish json string conveniently
    if (benchmark_result.takeRight(1) == ",")
      benchmark_result = benchmark_result.dropRight(1)
    benchmark_result = benchmark_result + "}"

    //if file path for benchmark is provided build and store benchmark results
    if (benchmark_file_path != null) {
      //store benchmark results
      val file = new File(benchmark_file_path)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(benchmark_result)
      bw.close()
    }
  }

}