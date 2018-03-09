package io.gitlab.leibnizhu.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.ProducerFactory;

public class HiveProducerFactory implements ProducerFactory {
    @Override
    public AbstractProducer createProducer(MaxwellContext maxwellContext) {
        return new HiveProducer(maxwellContext);
    }
}
