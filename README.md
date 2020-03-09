# Hadoop-MapRexuce

## commonFriend.java

Take input in the format of 

```
<User><TAB><Friends>
```

output mutul foriend for the rolloring userID pairs.
( 0,1 )( 20,28193 )( 1,29826 )( 6222,19272 )( 28041,28056 )

## TopTenCommonFriend.java

Take input in the format of 

```
<User><TAB><Friends>
```

output 10 user pairs thant has the most common friends.

## CommonFriendDOB.java

Take input in the format of 

```
<User><TAB><Friends>
```

And user data int the format of 

```
column1 : userid
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.

```

output of name and birthday of common friend of a user pair.

## MaxAge.java

Take input in the format of 
```
<User><TAB><Friends>
```

And user data int the format of 

```
column1 : userid
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.

```

And do the following.

Step 1: Calculate the maximum age of the direct friends of each user.
Step 2: Sort the users based on the calculated maximum age in descending order as described in step 1.
Step 3. Output the top 10 users from step 2 with their address and the calculated maximum age.


