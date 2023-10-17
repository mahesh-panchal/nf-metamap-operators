package nextflow.metaMapOperators

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint

/**
 * A plugin extension implementing potentially useful channel operators
 * for nf-core modules which use the meta map.
 *
 * @author : Mahesh Binzer-Panchal <mahesh.binzer-panchal@scilifelab.se>
 *
 */
@Slf4j
@CompileStatic
class metaMapOperatorsExtension extends PluginExtensionPoint {

    /*
     * A session hold information about current execution of the script
     */
    private Session session

    /*
     * nf-core initializes the plugin once loaded and session is ready
     * @param session
     */
    @Override
    protected void init(Session session) {
        this.session = session
    }

    /*
    * {@code groupTupleOnMetaKeys} is a *consumer* method as it receives values from a channel to perform some logic.
    *
    */
    @Operator
    DataflowWriteChannel groupTupleOnMetaKeys(final DataflowReadChannel source, final Map params=null) {
        def result = new GroupTupleByMetaKeysOp(params, source).apply()
        return result
    }
}
