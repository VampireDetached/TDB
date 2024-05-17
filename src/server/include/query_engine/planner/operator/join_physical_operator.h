#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

// TODO [Lab3] join算子的头文件定义，根据需要添加对应的变量和方法
class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator(std::unique_ptr<Expression> expression);
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

private:
  Trx *trx_ = nullptr;
  PhysicalOperator *left_ =nullptr;
  PhysicalOperator *right_=nullptr;
  Tuple            *left_tuple_  = nullptr;
  Tuple            *right_tuple_ = nullptr;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  unique_ptr<Expression> condition_;
};
