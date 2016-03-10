
package com.stratio.sparkta.plugin.output.solr

import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.{HttpSolrClient, CloudSolrClient}

object SolrOutputTest extends App{


  val asdsa: SolrClient = getSolrServer("isbanemf5.stratio.com:3181/solr", true)

  def getSolrServer(zkHost: String, isCloud: Boolean): SolrClient = {
    if (isCloud)
      new CloudSolrClient(zkHost)
    else
      new HttpSolrClient("http://" + zkHost + "/solr")
  }


}
