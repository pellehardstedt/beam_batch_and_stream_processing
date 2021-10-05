public class beamPipeline {

    public static void main(String[] args) {

        beamPipelineKafkaStream streamProcessing = new beamPipelineKafkaStream();
        streamProcessing.run();

        beamPipelineBatchCSV batchProcessing = new beamPipelineBatchCSV();
        batchProcessing.run();
    }
}
