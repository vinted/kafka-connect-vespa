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
@DocumentationImportant("This connector expects records from Kafka to have a key and value. Values can be converted "
        + "using byte, string or JSON converters. Topic names are used as document types, use single message "
        + "transforms to transform topic names into desired document types.")
@DocumentationNote("This connector supports deletes. If the record stored in Kafka has a null value, this connector "
        + "will delete document with the corresponding key to Vespa.")
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
