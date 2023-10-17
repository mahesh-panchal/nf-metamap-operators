// Derived from https://github.com/nextflow-io/nextflow/blob/master/modules/nextflow/src/main/groovy/nextflow/extension/GroupTupleOp.groovy

package nextflow.metaMapOperators

import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.util.ArrayBag
import nextflow.util.CacheHelper
import nextflow.util.CheckHelper
/**
 * Implements {@link OperatorImpl#groupTuple} operator logic
 *
 * @author Mahesh Binzer-Panchal <mahesh.binzer-panchal@scilifelab.se>
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class GroupTupleByMetaKeysOp {

    static private Map GROUP_TUPLE_PARAMS = [ 
        by: Integer, 
        sort: [Boolean, 'true','natural','deep','hash',Closure,Comparator], 
        size: Integer, 
        remainder: Boolean, 
        keys: [String, List] 
    ]

    static private List<Integer> GROUP_DEFAULT_INDEX = 0


    /**
     * Comparator used to sort tuple entries (when required)
     */
    private Comparator comparator

    private int size

    private Integer index

    private List<String> metaKeys

    private DataflowWriteChannel target

    private DataflowReadChannel channel

    private boolean remainder

    private Map<List,List> groups = [:]

    private sort

    GroupTupleByMetaKeysOp(Map params, DataflowReadChannel source) {

        CheckHelper.checkParams('groupTupleOnMetaKeys', params, GROUP_TUPLE_PARAMS)

        channel = source
        index = getGroupTupleIndex(params)
        metaKeys = getMetaKeys(params)
        size = params?.size ?: 0
        remainder = params?.remainder ?: false
        sort = params?.sort

        defineComparator()
    }

    GroupTupleOp setTarget(DataflowWriteChannel target) {
        this.target = target
        return this
    }

    static private List<String> getMetaKeys( Map params ) {
        if( params?.by == null )
            throw new IllegalArgumentException("Not a valid `keys` value for `groupTupleOnMetaKeys` operator: '${params.keys}' -- It must be a string or a list of strings defining keys in the meta map")
        if( params.by instance of List )
            return params.by as List<String>
        if( params.by instanceof String || params.by.toString() )
            return [ params.by as String ]

        throw new IllegalArgumentException("Not a valid `keys` value for `groupTupleOnMetaKeys` operator: '${params.keys}' -- It must be a string or a list of strings defining keys in the meta map")

    }

    static private Integer getGroupTupleIndex( Map params ) {

        if( params?.by == null )
            return GROUP_DEFAULT_INDEX

        if( params.by instanceof Integer || params.by.toString().isInteger() )
            return params.by as Integer

        throw new IllegalArgumentException("Not a valid `by` index for `groupTuple` operator: '${params.by}' -- It must be an integer value")
    }

    /*
     * Collects received values grouping by key
     */
    private void collect(List tuple) {

        final key = tuple[index].subMap(metaKeys).values() // the actual grouping key
        final len = tuple.size() 

        // groups is a map. The { ... } is a closure. This returns a List w. key??
        final List items = groups.getOrCreate(key) {    // get the group for the specified key
            def result = new ArrayList(len)             // create if does not exists
            for( int i=0; i<len; i++ )                  // maintains the position of the meta map in the tuple
                result[i] = (i == index ? tuple[i] : new ArrayBag()) 
            return result
        }

        int count=-1
        for( int i=0; i<len; i++ ) {                    // append the values in the tuple
            if( i != index ) {
                def list = (items[i] as List)
                if( list==null ) { // QUESTION: Is this redundant with the setting of the new ArrayBag() above?
                    list = new ArrayBag()
                    items.add(i, list)
                }
                list.add( tuple[i] )
                count=list.size()
            }
        }

        final sz = size ?: sizeBy(key)
        if( sz>0 && sz==count ) {
            bindTuple(items, sz)
            groups.remove(key)
        }
    }


    /*
     * finalize the grouping binding the remaining values
     */
    private void finalise(nop) {
        groups.each { keys, items -> bindTuple(items, size ?: sizeBy(keys)) }
        target.bind(Channel.STOP)
    }

    /*
     * bind collected items to the target channel
     */
    private void bindTuple( List items, int sz ) {

        def tuple = new ArrayList(items)

        if( !remainder && sz>0 ) {
            // verify exist it contains 'size' elements
            List list = items.find { it instanceof List }
            if( list.size() != sz ) {
                return
            }
        }

        if( comparator ) {
            sortInnerLists(tuple, comparator)
        }

        target.bind( tuple )
    }

    /**
     * Define the comparator to be used depending the #sort property
     */
    private void defineComparator( ) {

        /*
         * comparator logic used to sort tuple elements
         */
        switch(sort) {
            case null:
                break

            case true:
            case 'true':
            case 'natural':
                comparator = { o1,o2 -> o1<=>o2 } as Comparator
                break;

            case 'hash':
                comparator = { o1, o2 ->
                    def h1 = CacheHelper.hasher(o1).hash()
                    def h2 = CacheHelper.hasher(o2).hash()
                    return h1.asLong() <=> h2.asLong()
                } as Comparator
                break

            case 'deep':
                comparator = { o1, o2 ->
                    def h1 = CacheHelper.hasher(o1, CacheHelper.HashMode.DEEP).hash()
                    def h2 = CacheHelper.hasher(o2, CacheHelper.HashMode.DEEP).hash()
                    return h1.asLong() <=> h2.asLong()
                } as Comparator
                break

            case Comparator:
                comparator = sort as Comparator
                break

            case Closure:
                final closure = (Closure)sort
                if( closure.getMaximumNumberOfParameters()==2 ) {
                    comparator = sort as Comparator
                }
                else if( closure.getMaximumNumberOfParameters()==1 ) {
                    comparator = { o1, o2 ->
                        def v1 = closure.call(o1)
                        def v2 = closure.call(o2)
                        return v1 <=> v2
                    } as Comparator
                }
                else
                    throw new IllegalArgumentException("Invalid groupTuple option - The closure should have 1 or 2 arguments")
                break

            default:
                throw new IllegalArgumentException("Not a valid sort argument: ${sort}")
        }

    }

    /**
     * Main method to invoke the operator
     *
     * @return The resulting channel
     */
    DataflowWriteChannel apply() {

        if( target == null )
            target = CH.create()

        /*
         * apply the logic the the source channel
         */
        DataflowHelper.subscribeImpl(channel, [onNext: this.&collect, onComplete: this.&finalise])

        /*
         * return the target channel
         */
        return target
    }

    private static sortInnerLists( List tuple, Comparator c ) {

        for( int i=0; i<tuple.size(); i++ ) {
            def entry = tuple[i]
            if( !(entry instanceof List) ) continue
            Collections.sort(entry as List, c)
        }

    }

    static protected int sizeBy(List target)  {
        if( target.size()==1 && target[0] instanceof GroupKey ) {
            final group = (GroupKey)target[0]
            final size = group.getGroupSize()
            log.debug "GroupTuple dynamic size: key=${group} size=$size"
            return size
        }
        else
            return 0
    }

}
