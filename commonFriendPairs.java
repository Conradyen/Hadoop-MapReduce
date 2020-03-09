import org.apache.hadoop.io.Text;

public class commonFriendPairs {
    public Text key;
    public Text commonfriends;
    public int value;
    public commonFriendPairs(Text pair,Text commonfriends,int value){
        this.key = pair;
        this.commonfriends = commonfriends;
        this.value = value;
    }
}
