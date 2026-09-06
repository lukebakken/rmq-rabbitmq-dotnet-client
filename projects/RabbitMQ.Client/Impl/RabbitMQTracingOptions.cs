namespace RabbitMQ.Client
{
    /// <summary>
    /// Shapes the tracing spans this client produces. Applied through
    /// <see cref="RabbitMQActivitySource.TracingOptions"/>, which is process-wide.
    /// </summary>
    public class RabbitMQTracingOptions
    {
        /// <summary>
        /// When <see langword="true"/> (the default), the routing key is appended to publish and
        /// delivery span names, for example <c>publish my.routing.key</c>. Set it to
        /// <see langword="false"/> where a high-cardinality routing key would make span names
        /// unusable as an aggregation key.
        /// </summary>
        public bool UseRoutingKeyAsOperationName { get; set; } = true;

        /// <summary>
        /// When <see langword="true"/> (the default), a delivery span is parented to the trace
        /// context the publisher propagated in the message, so a publish and the deliveries it
        /// causes form a single trace.
        /// </summary>
        /// <remarks>
        /// Only parenting is affected. Whenever a context is successfully extracted from a message it
        /// is attached to the delivery span as an <see cref="System.Diagnostics.ActivityLink"/> as
        /// well, in both modes, so setting this to <see langword="false"/> does not replace a parent
        /// with a link - it drops the parent and leaves the link. Turn it off where the publisher and
        /// the consumer are better treated as separate traces, for example when one message fans out
        /// to many long-running consumers.
        /// </remarks>
        public bool UsePublisherAsParent { get; set; } = true;
    }
}
