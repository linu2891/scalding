package shoes
import com.twitter.scalding._
import cascading.pipe.joiner.LeftJoin
import com.twitter.scalding.FunctionImplicits._
/**
 * @author t
 */
class Recmd1(args: Args) extends Job(args) {

  val productsSchema = List('productId, 'brand, 'style, 'gender, 'primaryType_p1, 'type_p2, 'subType_p3, 'color)
  val catRecoSchema = List('primaryType_p1_reco, 'type_p2_reco, 'subType_p3_reco, 'RecommendedProductIds)

  val priceSchema = List('price_id, 'productId, 'maxSalePrice, 'minSalePrice)

  //  val logs = IterableSource[(String,String,String,String,String,String,String,String,String)](input, ('datetime, 'user, 'activity, 'data,
  //    'session, 'location, 'response, 'device))

  val products1 = Tsv(args("input"), productsSchema).read
  val catRecmnds = Tsv(args("inRCats"), catRecoSchema).read //recomdsCats.csv
  val products = products1 //.insert(('recoPrdt), (List()))
  val cats = products.unique('primaryType_p1, 'type_p2, 'subType_p3)
  println("cats made");
  // val productsJoin = products.joinWithLarger(fs, that, joiner, reducers);
  cats.write(Tsv("./o1/cats.csv"))
  println("cats wrote")
  products.write(Tsv("./o1/prpodcust1.csv"))
  println("write prod debug 1 ./o1/prpodcust1.csv")
  val jointProdWithRecoCat = products.joinWithSmaller(('primaryType_p1 -> 'primaryType_p1_reco), catRecmnds, new LeftJoin).addTrap(Tsv("./o1/join_pro-rec_err"))
  println("join with cat rec done")
  jointProdWithRecoCat.write(Tsv("./o1/prpodcust1AfterJoin1.csv"))

  val n = jointProdWithRecoCat.map(('productId, 'RecommendedProductIds) -> ('MyRecomdProducts, 'xx)) {
    //x : (String, String) => val(productId, RecommendedProductIds) = x ("bb","v")
    (productId: String, RecommendedProductIds: String) =>
      {
        val i = RecommendedProductIds.indexOf(productId)
        //string sub string or tokenize to list and remove depending on data quality and end format
        var myp = "";
        if (i < 0) {
          myp = RecommendedProductIds
        } else {
          var k = 0
          if (RecommendedProductIds.length() > (i + productId.length() + 1)) {
            k = RecommendedProductIds.indexOf(",", i + 1) + 1
          } else {
            k = i + productId.length()
          }
          myp = RecommendedProductIds.substring(0, i) + RecommendedProductIds.substring(k)
        }
        myp = myp.trim();
        if(myp.endsWith(",")){
          myp = myp.substring(0, myp.length() - 1)
        }
        (myp, "")
      }

    /*  v*/

  }
  n.write(Tsv("./o1/prpodcust1AfterJoin2.csv"))
  println("done");

}