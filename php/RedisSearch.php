<?php
/**
 *  该类是通用redis索引查询及更新类，用在有大量模糊查询的情况下，支持绝大部分的查询功能，如复合查询，模糊查询等，暂不支持范围查询，in查询，分组查询等。
 *  该类主要的方法就是查询接口和新增，删除接口，更新接口请直接使用删除+新增的方式实现。
 *  下面一一介绍接口的功能
 *  
 *  1、public function query($query,&$count,$page=0,$size=10,$getStored=false)
 *  查询接口，返回主键ID或者在getStore=true的时候，返回设置存储的字段集合.
 * 
 *  $query 是array,KV结构，查询的字段名字为K,查询该字段的值为V,
 *  etc: array('caption'=>'描述哦','status'=>-4)   caption是要查询的字段，是查询的值
 *
 *  $count 是引用传递，用来返回查询结果的总数
 *  $page是分页索引，从0开始
 *  $size是分页大小
 *  $getStored 是是否返回存储的字段，当添加索引时，使用 setStoreColumn的时候，这时候会把设置的字段存储成以ID为KEY的单个记录，返回的时候原样返回。
 *  接口使用示例：
 *      $redis=new ComRedis('search',0);
 *      $redis=$redis->getInstance();
 *      $db=Yii::app()->db_center;   *     
 *      $search=new RedisSearch();
 *      $search->setRedis($redis);//设置redis实例
 *      $search->setTableName($tableName); //设置表的名字，用来区分各种不同的业务查询
 *      $ret=$search->query($query,$count,0,100,true);
 *
 * 
 *  
 * 2、public function addIndex($id,array $columns_value)
 * 添加索引接口
 * $id是主键值
 * $columns_value 为记录的值，KV结构，K是列名,V是array, array[0]为列的值， array[1]为列对应的分数score，也就是排序字段值，必须是float类型
 * 接口使用示例：
 *      $redis=new ComRedis('search',0);
 *      $redis=$redis->getInstance();
 *      $db=Yii::app()->db_center;        
 *      $search=new RedisSearch();
 *      $search->setRedis($redis);
 *      $search->setTableName('kw');
 *      $search->setIndexColumn(array('keyword'=>false,'ad_user_id'=>false,'m_url'=>false,'c_url'=>false));
 *      $search->setStoreColumn(array('ad_user_id'));
 *      $data=$db->createCommand("select * from ad_search_keywords")->queryAll();           
 *     
 *      foreach ($data as $value) {
 *                    
 *          $id=strval($value['id']);               
 *          $score=floatval($value['id']);
 *          $keyword=strtolower(trim($value['keyword']));
 *         
 *          $url=strtolower(trim($value['website']));
 *          $urls=$this->_getUrl($url);
 *          $m_url=$urls[0];
 *          $c_url=$urls[1];         
 *          $columns=array(                    
 *              'keyword'=>array($keyword,$score),                    
 *              'ad_user_id'=>array($value['ad_user_id'],$score),
 *              'm_url'=>array($m_url,$score),
 *              'c_url'=>array($c_url,$score),
 *              );
 *          $search->addIndex($id,$columns);                   
 *      } 
 *
 * 3、其他接口请查看方法说明
 * 
 */

class RedisSearch
{
  //redis instant
  protected  $redis_context=null;
  //table name
  protected  $tableName=null; 
  //需要索引的列
  protected  $index_columns=array();
  //需要存储的列
  protected  $store_columns=array();
  //把store_column存储的值存到其他服务器上设置改项，若不设置，则和索引使用同一个redis
  protected $store_redis_context=null;

  /**
   * 设置redis对象，方法内需要使用redis进行数据交互
   * @param [type] $redis_context [description]
   */
  public function setRedis($redis_context)
  {
    if($redis_context==null)
      throw new Exception("can not set redis  null");
    $this->redis_context=$redis_context;
  }


  /**
   * 设置索引对象的表名，用来区分搜索类型，如advert或者keyword
   * @param [type] $tableName [description]
   */
  public function setTableName($tableName)
  {
    $this->tableName=$tableName;
  }

  /**
   * 设置哪些列的值是需要存储的，将来可以一并读取
   * @param array $columns [description]
   */
  public function setStoreColumn(array $columns)
  {
      $this->store_columns=$columns;        
  }

  /**
   * 设置存储设置了storecolumn记录的REDIS实例，若不设置，则自动使用索引的redis
   * @param [type] $redis_context [description]
   */
  public function setStoreRedis($redis_context)
  {
    if($redis_context==null)
      throw new Exception("can not set store redis  null");
    $this->store_redis_context=$redis_context;
  }

  /**
   * 需要索引的列，及列的索引模式
   * @param array $columns array('caption'=>true,'status'=>false)
   * KV结构，key是列名，V是是否需要分词索引，模糊查询时需要用这个
   */
  public function setIndexColumn(array $columns)
  {

    if($this->tableName==null)
      throw new Exception("set table name first");
    if(empty($columns))
      throw new Exception("column empty");
    $this->index_columns=$columns;
    $this->redis_context->set(':column_index_'.$this->tableName,serialize($columns));
  }



  /**
   * 查询方法
   *  
   * @param  [array] $query  etc: array('caption'=>'描述哦','status'=>-4)   caption是要查询的字段，是查询的值
   * @param  [integer]   count 总记录数，传递引用,用于分页
   * @param  [integer]   page 页数索引，从0开始
   * @param  [integer]   size 每页记录数大小
   * @param  [boolean]   getStored 是否取出存储数据
   * @return [array]     返回 主键ID  array,若getStore为true，则返回以id为key的，添加索引时存储的字段为value的数组。
   */
    public function query($query,&$count,$page=0,$size=10,$getStored=false)
    {
      $query_time_begin=microtime(true);
      if($this->redis_context==null)
      throw new Exception("use setRedis($redis) method to set redis context first!");

      $tableName=$this->tableName;
      if(!$query || !$tableName || $page<0 || $size<=0)
      {
        return array();
      }
      $redis_query=array();
      $key=":query_".$tableName.'_'.rand(0,99);//防止并发
      $columns=$this->getIndexColumn();
      foreach($query as $q=>$v)
      {           
        $redis_query=array_merge($redis_query,$this->getAscii($tableName,$q,$v,$columns[$q]));
      }
      $count=-1;
      $return=array();
      if($redis_query)
        {
          $count=$this->redis_context->zInter($key,$redis_query);
          if($count===false)
          {
               throw new Exception("redis zInter error,table:$tableName,key:$key,query:".json_encode($redis_query));
          }

          if($count>0)
          { 
            $ret=$this->redis_context->zrange($key,$page*$size,($page+1)*$size-1);
            if($ret===false)
            {
                 throw new Exception("redis zrange error,table:$tableName,key:$key,page:$page,size:$size");
            }

            if($ret)
            {
              
              $stored_keys=array();
              foreach($ret as $res)
              {
                if(strpos($res,$tableName)===false)
                {
                  throw new Exception('redis return no tableName prefix :'.$res);
                  break;
                }
                else{
                      
                      $id=intval(substr($res,strlen($tableName)+1));
                      if(!$getStored)
                      $return[]=$id;
                      else
                      {
                          $stored_keys[$id]=":item_".$tableName."_".$id;
                      } 
                }
              }

              if($stored_keys)
              {
                if($this->store_redis_context==null)
                    $contents=$this->redis_context->mget(array_values($stored_keys));
                else
                    $contents=$this->store_redis_context->mget(array_values($stored_keys));
                $i=0;
                foreach($stored_keys as $k=>$v)
                {
                   $content=$contents[$i]?unserialize($contents[$i]):null;
                   $return[]=array($k=>$content);
                   $i++;
                }
                unset($i);              
              }
            }
            $ret=null;
            unset($ret);
          }
        }

        $query_time_end=microtime(true);
        Utility::writeLog("[".date('Y-m-d H:i:s')."] Redis Query table:$tableName, end with duration:".intval(1000*($query_time_end-$query_time_begin))."ms,result count:$count,querystring:".json_encode($query), "RedisQuery.".date('Y-m-d').".log");
        return $return;
    }



    /**
     * 给表建立索引
     * @param [type] $id     主键id
     * @param array  $columns_value 是需要添加索引的项，格式 array('caption'=>array('这个是标题哦',float score))
     * id 必须要，主键   
     */
    public function addIndex($id,array $columns_value)
    {   
      if($this->redis_context==null)
      throw new Exception("set redis context first!");
      if($this->tableName==null)
      throw new Exception("set tableName first!");
      $columns=$this->getIndexColumn();

        //$id_arr=$columns_value['id'];
        unset($columns_value['id']);
        $id=strval($id);
        
    
        foreach (array_keys($columns_value) as $key) {
            $value=$columns_value[$key];
            $needSegment=false;
            if(isset($columns[$key]))
            {
                $needSegment=$columns[$key];
                $this->_addIndex($this->redis_context,$this->tableName,$id,$value[1],$key,$value[0],$needSegment);
            }
        }
        if($this->store_columns)
        {
            $tmp=array();
            foreach ($this->store_columns as  $value)
            {
                if(isset($columns_value[$value][0]))
                {
                     $tmp[$value]=$columns_value[$value][0];
                }
            }
            if($this->store_redis_context==null)
                $this->redis_context->set(":item_".$this->tableName."_".$id,serialize($tmp));
            else
                $this->store_redis_context->set(":item_".$this->tableName."_".$id,serialize($tmp));
        }
      Utility::writeLog("[".date('Y-m-d H:i:s')."] Redis addIndex end,table:".$this->tableName.",id:$id,columns_value:".json_encode($columns_value), "RedisQuery.".date('Y-m-d').".log");

    }

    /**
     * 删除一项索引
     * @param  [type] $id            该表的主键ID
     * @param  [type] $columns_value KV结构的列名对应值  array('status'=>0,'description'=>'这是描述部分'))
     * 示例：key=>value
     * @return [null]                null
     */
    public function delIndex($id,$columns_value)
    {
        if($this->redis_context==null)
        throw new Exception("set redis context first!");
        if($this->tableName==null)
        throw new Exception("set tableName first!");
        $index_columns=$this->getIndexColumn();

        if(count($index_columns)>count($columns_value))
            throw new Exception("columns count error!");

        foreach($columns_value as $k=>$v)
        {
          if(is_array($v))
              $v=$v[0];
          $keys=$this->getAscii($this->tableName,$k,$v,$index_columns[$k]);
              //var_dump($keys);
          foreach($keys as $key)
          {
            $column_key=$key; 
            $this->redis_context->zrem($column_key,$this->tableName."_".$id);
          }    
          if($index_columns[$k]==true)
          {
            $idkey="_".$this->tableName."_".$k."_id_".$id;
            $this->redis_context->del($idkey);
          }        
        }

        if($this->store_redis_context==null)
            $this->redis_context->del(":item_".$this->tableName."_".$id); 
        else
            $this->store_redis_context->del(":item_".$this->tableName."_".$id); 

        Utility::writeLog("[".date('Y-m-d H:i:s')."] Redis delIndex end,table:".$this->tableName.",id:$id,columns_value:".json_encode($columns_value), "RedisQuery.".date('Y-m-d').".log");  
    }  

    /**
     * 获取索引的列
     * @return [type] [description]
     */
    protected function getIndexColumn()
    {

      if($this->index_columns!=null)
      return $this->index_columns;
      else
      {
        $ret=$this->redis_context->get(':column_index_'.$this->tableName);
        if($ret)
        {
          $ret=unserialize($ret);
          $this->index_columns=$ret;
          return $ret;
        }
        else
          throw new Exception("index column has not been set");

      }
    }

  
    /**
     * 添加索引方法
     * @param [type] $redis       redis 实例
     * @param [type] $tableName   表名
     * @param [type] $id          主键ID
     * @param [type] $score       排序分数
     * @param [type] $columnName  列名
     * @param [type] $columnValue 列值
     * @param [type] $needSegment 该列是否需要分词（分词可以模糊查询）
     */
    protected function _addIndex($redis,$tableName,$id,$score,$columnName,$columnValue,$needSegment)
    {
        if(!$tableName || !$id  || !$columnName || !$redis )
            return false;

        $score=floatval($score);
        $ascii=$this->getAscii($tableName,$columnName,$columnValue,$needSegment);

        $setvalue=array();        
        $idval=$tableName."_".$id;
       
        foreach($ascii as $asc)
        {  
           $redis->zAdd($asc,$score,$idval);
           $s=explode('_',$asc);
           $setvalue[]=$s[count($s)-1];
        }
        if($needSegment==true)
        {
            $idkey="_".$tableName."_".$columnName."_id_".$id;
            
            
            $redis->set($idkey,",".implode(",",$setvalue).",");        
        }
        
        return true;

    }



     /**
     * 统一的分词方法
     * @param  [type]  $tableName   [description]
     * @param  [type]  $columnName  [description]
     * @param  [type]  $columnValue [description]
     * @param  boolean $needSegment 是否需要分词。不需要分词的，str不要传中文
     * @return [type]               [description]
     */
    protected function getAscii($tableName,$columnName,$columnValue,$needSegment=true)
    {
       
        $columnValue=strtolower(str_replace("_","",$columnValue));
        if($columnValue==="")
            return array();
        $w=array();
        if($needSegment!=true)
        {
            // $chinease=array();           
            // $res=preg_match_all("/[\x{4e00}-\x{9fa5}]/u",$columnValue,$chinease);
            
            // if($chinease && count($chinease)==1)
            // {
            //     $chinease=array_unique($chinease[0]);
            //     foreach ($chinease as $value) {
            //            $columnValue =str_replace($value,dechex($this->utf8_unicode($value))."_", $columnValue) ;
            //     }
            // }
            if(!is_numeric($columnValue))
     		   $columnValue=$this->dec2s4(hexdec(substr(md5($columnValue),0,12)));//取48位的md5码，转换成64进制数据

            return array($tableName."_".$columnName."_".$columnValue);
        }        
        //如果需要分词       
       
        preg_match_all("/([0-9]*)([a-z]*)([\x{4e00}-\x{9fa5}]*)/u", $columnValue, $w,PREG_OFFSET_CAPTURE);
        
        if(count($w)==4)
            {
                $tmp=array();
                unset($w[0]);
                $seged=array();
                foreach($w as $index=> $ks)
                {
                    foreach($ks as $k)
                    {
                        if(!($k[0]===""))
                        {
                          //index 1是数字，2是字母 3是中文
                          $seged[$k[1]]=array($k[0],$index);
                        }
                    }
                }
                //按照之前的offset 得到原来每个分词的顺序
                ksort($seged);

                foreach(array_keys($seged) as $key)
                {
                    $value=$seged[$key];
                    if($value[1]==3)
                    {
                      //如果是中文                      
                        $s=array();
                        preg_match_all("/[\x{4e00}-\x{9fa5}]/u", $value[0], $s);

                        $s2=array();
                        if(count($s[0])<3)
                            $s2=array($s[0]);
                        else
                        {
                            for($i=0;$i<count($s[0])-1;$i++)
                            {
                                $s2[]=array_slice($s[0],$i,2);
                            }                    
                        }
                       
                       
                        foreach($s2 as $single)
                        {
                            $t="";
                            foreach ($single as  $value) {
                                $t .= dechex($this->utf8_unicode($value));
                            }
                            if($t)
                            $tmp[]="_".$tableName."_".$columnName."_".$t;                   
                        }
                      
                    }
                    else
                    {
                       $tmp[]="_".$tableName."_".$columnName."_".$value[0];
                    }
                }                
               
                return $tmp;
            }
        else
            return array();

    }



    protected function utf8_unicode($c) {
         switch(strlen($c)) {
             case 1:
             return ord($c);
             case 2:
             $n = (ord($c[0]) & 0x3f) << 6;
             $n += ord($c[1]) & 0x3f;
             return $n;
             case 3:
             $n = (ord($c[0]) & 0x1f) << 12;
             $n += (ord($c[1]) & 0x3f) << 6;
             $n += ord($c[2]) & 0x3f;
             return $n;
             case 4:
             $n = (ord($c[0]) & 0x0f) << 18;
             $n += (ord($c[1]) & 0x3f) << 12;
             $n += (ord($c[2]) & 0x3f) << 6;
             $n += ord($c[3]) & 0x3f;
             return $n;
    }
  }
  	//10进制转64进制
    protected function dec2s4($dec) {
        $base = '0123456789:;abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        $result = ''; 
        do {
            $result = $base[$dec % 64] . $result;
            $dec = intval($dec / 64);
        } while ($dec != 0); 

        return $result;
    }  

}