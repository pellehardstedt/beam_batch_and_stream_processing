import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class beamPipelineBatchCSV extends Thread {

    public beamPipelineBatchCSV() {
    }

    static class removeEan extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver ){
            String[] columns = element.split(",");
            String stringOut = "";
            for (int i = 1; i < columns.length; i++) {
                stringOut = stringOut.concat(columns[i]);
            }
            receiver.output(stringOut);
        }
    }

    public void run(){
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create();
        p.apply("read csv", TextIO.read().from("C:\\Users\\Pelle\\Desktop\\productsPipeline\\src\\main\\resources\\upc_corpus - Copy.csv"))
                .apply("transform", ParDo.of(new removeEan()))
                .apply("write csv", TextIO.write().to("C:\\Users\\Pelle\\Desktop\\productsPipeline\\src\\main\\resources\\upc_corpus_new.csv"));
        try {
            p.run().waitUntilFinish();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}