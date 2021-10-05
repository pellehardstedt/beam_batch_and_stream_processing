import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;


public class beamPipelineKafkaStream extends Thread {

    public beamPipelineKafkaStream(){
    }

    static class oneLog extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver ){
            System.out.println(element);
            receiver.output(element);
        }
    }

    public void run(){
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create();

        PCollection<String> message = p.apply(KafkaIO.<String,String>read()
                .withBootstrapServers("localhost:9092,localhost:9093,localhost:9094")
                .withTopic("rsvp")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata())
                .apply(Values.<String>create())
                .apply(ParDo.of(new oneLog()));

        p.run().waitUntilFinish();
    }
}