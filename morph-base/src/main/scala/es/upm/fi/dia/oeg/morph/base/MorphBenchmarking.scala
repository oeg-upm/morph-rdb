package es.upm.fi.dia.oeg.morph.base

import java.io._

class MorphBenchmarking {
  // Times for the different OBDA phases (according to publication: The NPD Benchmark for OBDA Systems)
  // These times must be in miliseconds
  private var _starting_time:Long = -1
  private var _rewriting_time:Long = -1
  private var _translation_time:Long = -1
  private var _execution_time:Long = -1

  private var _materialization_time:Long = -1

  //When each of the OBDA phases start, current_phase_start_time is updated
  private var _current_phase_start_time:Long = System.currentTimeMillis()

  def startRewritingPhase() = {
    val current_time_millis = System.currentTimeMillis()
    this._starting_time = current_time_millis - this._current_phase_start_time
    //update the current_phase_start_time of the next OBDA phase
    this._current_phase_start_time = current_time_millis
  }

  def startTranslationPhase() = {
    val current_time_millis = System.currentTimeMillis()
    this._rewriting_time = current_time_millis - this._current_phase_start_time
    //update the current_phase_start_time of the next OBDA phase
    this._current_phase_start_time = current_time_millis
  }

  def startExecutionPhase() = {
    val current_time_millis = System.currentTimeMillis()
    this._translation_time = current_time_millis - this._current_phase_start_time
    //update the current_phase_start_time of the next OBDA phase
    this._current_phase_start_time = current_time_millis
  }

  def finishBenchmarking() = {
    this._execution_time = System.currentTimeMillis() - this._current_phase_start_time
    //no more OBDA phases to benchmark, current_phase_start_time not updated

    //store benchmark results
    val file = new File("benchmark.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    if (_starting_time != -1) bw.write("Starting time: " + _starting_time.toString() + "ms.")
    if (_rewriting_time != -1) bw.write("Rewriting time: " + _rewriting_time.toString() + "ms.")
    if (_translation_time != -1) bw.write("Translation time: " + _translation_time.toString() + "ms.")
    if (_execution_time != -1) bw.write("Execution time: " + _execution_time.toString() + "ms.")
    bw.close()
  }

}
