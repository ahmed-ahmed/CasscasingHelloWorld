//import cascading.flow.FlowDef;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlow;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Debug;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;



public class MyApp {
    public static void main(String[] args) {
        String authorPath = args[0];
        String employeePath = args[1];
        String outputPath = args[2];

        Fields author = new Fields("author", "organization", "document", "keyword");
        Fields emplyee = new Fields("department", "employee");
        Fields output = new Fields("organization", "department", "employee", "document", "keyword");

        Scheme authorSource = new TextDelimited(author, true, ",");
        Scheme employeeSource = new TextDelimited(emplyee, true, ",");

        Tap authorTap = new FileTap(authorSource, authorPath);
        Tap employeeTap = new FileTap(employeeSource, employeePath);


        Scheme sinkScheme = new TextDelimited(output, true, ",");
        Tap sinkTap = new FileTap(sinkScheme, outputPath, SinkMode.REPLACE);


        Pipe authorPipe = new Pipe("authorPipe");
        authorPipe = new Each(authorPipe, new Debug());
        Pipe employeePipe = new Pipe("employeePipe");
        employeePipe = new Each(employeePipe, new Debug());


        Pipe join = new CoGroup(authorPipe, new Fields("author"), employeePipe, new Fields("employee"), new InnerJoin());

        join = new Each(join, output, new Identity());


        FlowDef flowDef = FlowDef.flowDef()
                .addSource(authorPipe, authorTap)
                .addSource(employeePipe, employeeTap)
                .addTailSink(join, sinkTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}