package com.vinted.kafka.connect.vespa;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Title("Kafka Connect Vespa Sink Connector")
@Description("The Vespa Sink Connector is used to write data from Kafka to a Vespa search engine.")
@DocumentationImportant("Values can be strings or bytes holding JSON, or schema-based data from any converter, such as "
        + "Avro with Schema Registry, whose converter is bundled. In UPSERT mode the key is the document id and the "
        + "namespace and document type default to the topic name; in RAW mode each value is a Vespa document "
        + "operation and the key is not used.")
@DocumentationNote("In UPSERT mode a record with a null value deletes the document with its key. In RAW mode such "
        + "records are skipped; send a remove operation instead.")
public class VespaSinkConnector extends SinkConnector {
    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return VespaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(new HashMap<>(props));
        }

        return ImmutableList.copyOf(taskConfigs);
    }

    @Override
    public void stop() {
        // Do nothing.
    }

    @Override
    public ConfigDef config() {
        return VespaSinkConfig.CONFIG;
    }

    @Override
    public String version() {
        return VespaSinkConnectorVersion.getVersion();
    }
}
