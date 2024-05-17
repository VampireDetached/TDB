#include "include/query_engine/planner/operator/join_physical_operator.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

// JoinPhysicalOperator::JoinPhysicalOperator() = default;

JoinPhysicalOperator::JoinPhysicalOperator(std::unique_ptr<Expression> expr) : condition_(std::move(expr))
{
  // ASSERT(expression_->value_type() == BOOLEANS, "predicate's expression should be BOOLEAN type");
}

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  trx_ = trx;
  
  left_ = children_[0].get();
  right_ = children_[1].get();
  children_[0]->open(trx);
  children_[1]->open(trx);
  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
  RC rc;
  while(right_->next() != RC::RECORD_EOF)
  {
    right_tuple_ = right_->current_tuple();
    left_->close();
    left_->open(trx_);
    while(left_->next() != RC::RECORD_EOF)
    {
      left_tuple_ = left_->current_tuple();
      JoinedTuple joined_tuple;
      joined_tuple.set_left(left_tuple_);
      joined_tuple.set_right(right_tuple_);
      Value value;
      rc = condition_->get_value(joined_tuple, value);
      if(rc != RC::SUCCESS)
      {
        return rc;
      }
      if (value.get_boolean()) {
        joined_tuple_ = joined_tuple;
        return rc;
      }
    }
  }
  
  return RC::RECORD_EOF;
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  left_=nullptr;
  right_=nullptr;
  left_tuple_=nullptr;
  right_tuple_=nullptr;
  return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
