#include <cassert>
#include <functional>
#include <vector>
#include <string>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include "moderndbs/algebra.h"

namespace moderndbs {
    namespace iterator_model {

/// This can be used to store registers in an `std::unordered_map` or
/// `std::unordered_set`. Examples:
///
/// std::unordered_map<Register, int, RegisterHasher> map_from_reg_to_int;
/// std::unordered_set<Register, RegisterHasher> set_of_regs;
        struct RegisterHasher {
            uint64_t operator()(const Register& r) const {
                return r.get_hash();
            }
        };


/// This can be used to store vectors of registers (which is how tuples are
/// represented) in an `std::unordered_map` or `std::unordered_set`. Examples:
///
/// std::unordered_map<std::vector<Register>, int, RegisterVectorHasher> map_from_tuple_to_int;
/// std::unordered_set<std::vector<Register>, RegisterVectorHasher> set_of_tuples;
        struct RegisterVectorHasher {
            uint64_t operator()(const std::vector<Register>& registers) const {
                std::string hash_values;
                for (auto& reg : registers) {
                    uint64_t hash = reg.get_hash();
                    hash_values.append(reinterpret_cast<char*>(&hash), sizeof(hash));
                }
                return std::hash<std::string>{}(hash_values);
            }
        };


        Register Register::from_int(int64_t value) {
            Register reg{};
            reg.intValue = value;
            return reg;
        }


        Register Register::from_string(const std::string& value) {
            Register reg{};
            reg.stringValue = value;
            return reg;
        }


        Register::Type Register::get_type() const {
            if (this->intValue) {
                return Type::INT64;
            } else {
                return  Type::CHAR16;
            }
        }


        int64_t Register::as_int() const {
            if (this->intValue) {
                return *this->intValue;
            } else {
                return 0;
            }
        }


        std::string Register::as_string() const {
            if (this->stringValue) {
                return *this->stringValue;
            } else {
                return std::string{};
            }
        }


        uint64_t Register::get_hash() const {
            if (this->get_type() == Type::INT64) {
                return std::hash<int64_t>{}(*this->intValue);
            } else {
                return std::hash<std::string>{}(*this->stringValue);
            }
        }


        bool operator==(const Register& r1, const Register& r2) {
            return r1.get_hash() == r2.get_hash();
        }


        bool operator!=(const Register& r1, const Register& r2) {
            return r1.get_hash() != r2.get_hash();
        }


        bool operator<(const Register& r1, const Register& r2) {
            assert(r1.get_type() == r2.get_type());
            if (r1.get_type() == Register::Type::INT64) {
                return r1.as_int() < r2.as_int();
            } else {
                return r1.as_string().compare(r2.as_string()) < 0;
            }
        }


        bool operator<=(const Register& r1, const Register& r2) {
            assert(r1.get_type() == r2.get_type());
            if (r1.get_type() == Register::Type::INT64) {
                return r1.as_int() <= r2.as_int();
            } else {
                return r1.as_string().compare(r2.as_string()) <= 0;
            }
        }


        bool operator>(const Register& r1, const Register& r2) {
            assert(r1.get_type() == r2.get_type());
            if (r1.get_type() == Register::Type::INT64) {
                return r1.as_int() > r2.as_int();
            } else {
                return r1.as_string().compare(r2.as_string()) > 0;
            }
        }


        bool operator>=(const Register& r1, const Register& r2) {
            assert(r1.get_type() == r2.get_type());
            if (r1.get_type() == Register::Type::INT64) {
                return r1.as_int() >= r2.as_int();
            } else {
                return r1.as_string().compare(r2.as_string()) >= 0;
            }
        }


        Print::Print(Operator& input, std::ostream& stream) : UnaryOperator(input) {
            this->stream = &stream;
        }

        Print::~Print() = default;


        void Print::open() {
            this->input->open();
        }


        bool Print::next() {
            if (this->input->next()) {
                std::vector<Register*> regs = this->input->get_output();
                std::string str = "";
                for (auto& reg : regs) {
                    if (reg->get_type() == Register::Type::INT64) {
                        str += std::to_string(reg->as_int()) + ",";
                    } else {
                        str += reg->as_string() + ",";
                    }
                }
                if (regs.size() > 0) {
                    str = str.substr(0, str.size()-1);
                    str += '\n';
                    *this->stream << str;
                }
                return true;
            } else {
                return false;
            }
        }


        void Print::close() {
            this->input->close();
        }


        std::vector<Register*> Print::get_output() {
            // Print has no output
            return {};
        }


        Projection::Projection(Operator& input, std::vector<size_t> attr_indexes)
                : UnaryOperator(input) {
            this->attr_indexes = std::move(attr_indexes);
        }


        Projection::~Projection() = default;


        void Projection::open() {
            this->input->open();
        }


        bool Projection::next() {
            if (this->input->next()) {
                std::vector<Register*> regs = this->input->get_output();
                for (auto attr_index : this->attr_indexes) {
                    this->output_regs.push_back(*regs[attr_index]);
                }
                return true;
            } else {
                return false;
            }
        }


        void Projection::close() {
            this->input->close();
        }


        std::vector<Register*> Projection::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        Select::Select(Operator& input, PredicateAttributeInt64 predicate)
                : UnaryOperator(input) {
            this->predicateAttribute = Select::PrecidateAttribute::INT;
            this->intPredicate = predicate;
        }


        Select::Select(Operator& input, PredicateAttributeChar16 predicate)
                : UnaryOperator(input) {
            this->predicateAttribute = Select::PrecidateAttribute::CHAR;
            this->charPredicate = std::move(predicate);
        }


        Select::Select(Operator& input, PredicateAttributeAttribute predicate)
                : UnaryOperator(input) {
            this->predicateAttribute = Select::PrecidateAttribute::ATTRIBUTE;
            this->attributePredicate = predicate;
        }


        Select::~Select() = default;


        void Select::open() {
            this->input->open();
        }


        bool Select::next() {
            if (this->input->next()) {
                std::vector<Register*> regs = this->input->get_output();
                switch (this->predicateAttribute) {
                    case PrecidateAttribute::INT :
                        switch (this->intPredicate.predicate_type) {
                            case Select::PredicateType::EQ :
                            {
                                auto& intReg = regs[this->intPredicate.attr_index];
                                Register int_reg = Register::from_int(this->intPredicate.constant);
                                if (*intReg == int_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::NE :
                            {
                                auto& intReg = regs[this->intPredicate.attr_index];
                                Register int_reg = Register::from_int(this->intPredicate.constant);
                                if (*intReg != int_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::LT :
                            {
                                auto& intReg = regs[this->intPredicate.attr_index];
                                Register int_reg = Register::from_int(this->intPredicate.constant);
                                if (*intReg < int_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::LE :
                            {
                                auto& intReg = regs[this->intPredicate.attr_index];
                                Register int_reg = Register::from_int(this->intPredicate.constant);
                                if (*intReg <= int_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::GT :
                            {
                                auto& intReg = regs[this->intPredicate.attr_index];
                                Register int_reg = Register::from_int(this->intPredicate.constant);
                                if (*intReg > int_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::GE :
                            {
                                auto& intReg = regs[this->intPredicate.attr_index];
                                Register int_reg = Register::from_int(this->intPredicate.constant);
                                if (*intReg >= int_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                        }
                    case PrecidateAttribute::CHAR :
                        switch (this->charPredicate.predicate_type) {
                            case Select::PredicateType::EQ :
                            {
                                auto& stringReg = regs[this->charPredicate.attr_index];
                                Register str_reg = Register::from_string(this->charPredicate.constant);
                                if (*stringReg == str_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::NE :
                            {
                                auto& stringReg = regs[this->charPredicate.attr_index];
                                Register str_reg = Register::from_string(this->charPredicate.constant);
                                if (*stringReg != str_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::LT :
                            {
                                auto& stringReg = regs[this->charPredicate.attr_index];
                                Register str_reg = Register::from_string(this->charPredicate.constant);
                                if (*stringReg < str_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::LE :
                            {
                                auto& stringReg = regs[this->charPredicate.attr_index];
                                Register str_reg = Register::from_string(this->charPredicate.constant);
                                if (*stringReg <= str_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::GT :
                            {
                                auto& stringReg = regs[this->charPredicate.attr_index];
                                Register str_reg = Register::from_string(this->charPredicate.constant);
                                if (*stringReg > str_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::GE :
                            {
                                auto& stringReg = regs[this->charPredicate.attr_index];
                                Register str_reg = Register::from_string(this->charPredicate.constant);
                                if (*stringReg >= str_reg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                        }
                    case PrecidateAttribute::ATTRIBUTE :
                        switch (this->attributePredicate.predicate_type) {
                            case Select::PredicateType::EQ :
                            {
                                Register* leftReg = regs[this->attributePredicate.attr_left_index];
                                Register* rightReg = regs[this->attributePredicate.attr_right_index];
                                if (*leftReg == *rightReg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::NE :
                            {
                                Register* leftReg = regs[this->attributePredicate.attr_left_index];
                                Register* rightReg = regs[this->attributePredicate.attr_right_index];
                                if (*leftReg != *rightReg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::LT :
                            {
                                Register* leftReg = regs[this->attributePredicate.attr_left_index];
                                Register* rightReg = regs[this->attributePredicate.attr_right_index];
                                if (*leftReg < *rightReg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::LE :
                            {
                                Register* leftReg = regs[this->attributePredicate.attr_left_index];
                                Register* rightReg = regs[this->attributePredicate.attr_right_index];
                                if (*leftReg <= *rightReg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::GT :
                            {
                                Register* leftReg = regs[this->attributePredicate.attr_left_index];
                                Register* rightReg = regs[this->attributePredicate.attr_right_index];
                                if (*leftReg > *rightReg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                            case Select::PredicateType::GE :
                            {
                                Register* leftReg = regs[this->attributePredicate.attr_left_index];
                                Register* rightReg = regs[this->attributePredicate.attr_right_index];
                                if (*leftReg >= *rightReg) {
                                    for (auto r : regs) {
                                        this->output_regs.push_back(*r);
                                    }
                                }
                            }
                                return true;
                        }
                }
            } else {
                return false;
            }
        }


        void Select::close() {
            this->input->close();
        }


        std::vector<Register*> Select::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        Sort::Sort(Operator& input, std::vector<Criterion> criteria)
                : UnaryOperator(input) {
            this->criteria = std::move(criteria);
        }


        Sort::~Sort() = default;


        void Sort::open() {
            this->input->open();
        }


        bool Sort::next() {
            if (!this->isMaterialized) {
                while (this->input->next()) {
                    std::vector<Register*> regs = this->input->get_output();
                    std::vector<Register> output_regs;
                    output_regs.reserve(regs.size());
                    for (auto& reg : regs) {
                        output_regs.push_back(*reg);
                    }
                    this->registers.push_back(output_regs);
                }
                for(Criterion c : this->criteria) {
                    if (c.desc) {
                        std::sort(this->registers.begin(), this->registers.end(),
                                  [&c](const std::vector<Register>& regs1, const std::vector<Register>& regs2) {
                                      return regs1[c.attr_index] > regs2[c.attr_index];
                                  });
                    }
                }
                this->isMaterialized = true;
            }
            if (this->current_index < this->registers.size()) {
                return true;
            }
            return false;
        }


        std::vector<Register*> Sort::get_output() {
            std::vector<Register*> output;
            auto regs = this->registers[this->current_index];
            output.reserve(regs.size());
            ++this->current_index;
            for (auto& reg : regs) {
                output.push_back(&reg);
            }
            return output;
        }


        void Sort::close() {
            this->input->close();
        }


        HashJoin::HashJoin(
                Operator& input_left,
                Operator& input_right,
                size_t attr_index_left,
                size_t attr_index_right
        ) : BinaryOperator(input_left, input_right) {
            this->attr_index_left = attr_index_left;
            this->attr_index_right = attr_index_right;
        }


        HashJoin::~HashJoin() = default;


        void HashJoin::open() {
            this->input_left->open();
            this->input_right->open();
        }

        /// I know that I should insert left and right tuples into hash tables
        /// then for each item in left hash table I have to check whether that key exists
        /// in right hash table if yes, add all elements in register vector from both left
        /// and right has table into output_regs.
        /// However unordered_set was always empty after each insert and I was not getting any error
        /// I couldn't understand the problem
        bool HashJoin::next() {
            while (this->input_right->next()) {
                std::vector<Register> regs;
                for (auto& reg : this->input_right->get_output()) {
                    regs.push_back(*reg);
                }
                this->registers.push_back(regs);
            }
            while(true) {
                if (this->step) {
                    if (this->input_left->next()) {
                        this->left_regs = this->input_left->get_output();
                        this->step = false;
                    } else {
                        return false;
                    }
                }
                if (this->current_index < this->registers.size()) {
                    this->right_regs = this->registers[this->current_index];
                    if (*this->left_regs[this->attr_index_left] == this->right_regs[this->attr_index_right]) {
                        for (auto& reg : this->left_regs) {
                            this->output_regs.push_back(*reg);
                        }
                        for (auto& reg : this->right_regs) {
                            this->output_regs.push_back(reg);
                        }
                    }
                    ++this->current_index;
                    return true;
                }
                this->current_index = 0;
                step = true;
            }
        }


        void HashJoin::close() {
            this->input_left->close();
            this->input_right->close();
        }


        std::vector<Register*> HashJoin::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        HashAggregation::HashAggregation(
                Operator& input,
                std::vector<size_t> group_by_attrs,
                std::vector<AggrFunc> aggr_funcs
        ) : UnaryOperator(input) {
            this->group_by_attrs = std::move(group_by_attrs);
            this->aggr_funcs = std::move(aggr_funcs);
        }


        HashAggregation::~HashAggregation() = default;


        void HashAggregation::open() {
            this->input->open();
        }


        bool HashAggregation::next() {
            std::experimental::optional<Register> minRegister;
            std::experimental::optional<Register> maxRegister;
            std::unordered_map<Register, int, RegisterHasher> countMap;
            std::unordered_map<Register, int, RegisterHasher> sumMap;
            if (!this->isMaterialized) {
                while (this->input->next()) {
                    std::vector<Register *> regs = this->input->get_output();
                    for (AggrFunc func : this->aggr_funcs) {
                        switch (func.func) {
                            case AggrFunc::MIN:
                                if (!minRegister) {
                                    minRegister = *regs[func.attr_index];
                                } else {
                                    if (*regs[func.attr_index] < *minRegister) {
                                        minRegister = *regs[func.attr_index];
                                    }
                                }
                                break;
                            case AggrFunc::MAX:
                                if (!maxRegister) {
                                    maxRegister = *regs[func.attr_index];
                                } else {
                                    if (*regs[func.attr_index] > *maxRegister) {
                                        maxRegister = *regs[func.attr_index];
                                    }
                                }
                                break;
                            case AggrFunc::COUNT:
                                for (size_t attr : this->group_by_attrs) {
                                    auto it = countMap.find(*regs[attr]);
                                    if (it == countMap.end()) {
                                        countMap.insert({*regs[attr], 1});
                                    } else {
                                        countMap[*regs[attr]] += 1;
                                    }
                                }
                                break;
                            case AggrFunc::SUM:
                                for (size_t attr : this->group_by_attrs) {
                                    Register myreg = *regs[attr];
                                    auto it = sumMap.find(myreg);
                                    Register r = *regs[func.attr_index];
                                    if (it == sumMap.end()) {
                                        sumMap.insert({myreg, static_cast<int>(r.as_int())});
                                    } else {
                                        sumMap[myreg] += static_cast<int>(r.as_int());
                                    }
                                }
                                break;
                        }
                    }

                }
                if (sumMap.size() > 0) {
                    std::vector<Register> keys;
                    keys.reserve(sumMap.size());
                    for (auto it : sumMap) {
                        keys.push_back(it.first);
                    }
                    this->numberOfKeys = static_cast<int>(keys.size());
                    std::sort(keys.begin(), keys.end());
                    for (auto& it : keys) {
                        std::vector<Register> reg_vector;
                        reg_vector.push_back(it);
                        auto sumRegister = Register::from_int(sumMap[it]);
                        reg_vector.push_back(sumRegister);
                        auto countRegister = Register::from_int(countMap[it]);
                        reg_vector.push_back(countRegister);
                        this->temp_sumcount_registers.push_back(reg_vector);
                        /*std::cout << " " << it.as_int() << " sum: " << sumMap[it] <<
                                  " count: " << countMap[it];
                        std::cout << std::endl;*/
                    }
                }
                this->isMaterialized = true;
            }
            if (minRegister) {
                this->output_regs.push_back(*minRegister);
                if (maxRegister) {
                    this->output_regs.push_back(*maxRegister);
                }
                return true;
            }
            if (this->counter_index < this->numberOfKeys) {
                this->output_regs = this->temp_sumcount_registers[this->counter_index];
                ++this->counter_index;
                return true;
            }
            return false;
        };


        void HashAggregation::close() {
            this->input->close();
        }


        std::vector<Register*> HashAggregation::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        Union::Union(Operator& input_left, Operator& input_right)
                : BinaryOperator(input_left, input_right) {
        }


        Union::~Union() = default;


        void Union::open() {
           this->input_left->open();
           this->input_right->open();
        }


        bool Union::next() {
            if (!this->isMaterialized) {
                std::unordered_map<Register, int, RegisterHasher> registers_map;
                while (this->input_left->next()) {
                    std::vector<Register*> regs = this->input_left->get_output();
                    for (auto it : regs) {
                        auto got = registers_map.find(*it);
                        if (got == registers_map.end()) {
                            registers_map.insert({*it, 1});
                        } else {
                            registers_map[*it] += 1;
                        }
                    }
                }

                while (this->input_right->next()) {
                    std::vector<Register*> regs = this->input_right->get_output();
                    for (auto it : regs) {
                        auto got = registers_map.find(*it);
                        if (got == registers_map.end()) {
                            registers_map.insert({*it, 1});
                        } else {
                            registers_map[*it] += 1;
                        }
                    }
                }
                /*std::cout << "right side" << std::endl;
                for (auto it : right_registers)
                    std::cout << " " << it.first.get_hash() << ":" << it.second;

                std::cout << '\n';
                std::cout << '\n' << "right side end" << std::endl;*/

                for (auto it : registers_map) {
                    this->registers.push_back(it.first);
                }
                std::sort( this->registers.begin(), this->registers.end(),
                           [ ]( const Register& lhs, const Register& rhs ) {
                               return lhs < rhs;
                           });
                this->isMaterialized = true;
            }
            if (this->counter_index < static_cast<int>(this->registers.size())) {
                this->output_regs.push_back(this->registers[this->counter_index]);
                this->counter_index++;
                return true;
            } else {
                return false;
            }
        }


        std::vector<Register*> Union::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        void Union::close() {
            this->input_left->close();
            this->input_right->close();
        }


        UnionAll::UnionAll(Operator& input_left, Operator& input_right)
                : BinaryOperator(input_left, input_right) {
        }


        UnionAll::~UnionAll() = default;


        void UnionAll::open() {
            this->input_left->open();
            this->input_right->open();
        }


        bool UnionAll::next() {
            if (!this->isMaterialized) {
                std::unordered_map<Register, int, RegisterHasher> registers_map;
                while (this->input_left->next()) {
                    std::vector<Register*> regs = this->input_left->get_output();
                    for (auto it : regs) {
                        auto got = registers_map.find(*it);
                        if (got == registers_map.end()) {
                            registers_map.insert({*it, 1});
                        } else {
                            registers_map[*it] += 1;
                        }
                    }
                }

                while (this->input_right->next()) {
                    std::vector<Register*> regs = this->input_right->get_output();
                    for (auto it : regs) {
                        auto got = registers_map.find(*it);
                        if (got == registers_map.end()) {
                            registers_map.insert({*it, 1});
                        } else {
                            registers_map[*it] += 1;
                        }
                    }
                }

                for (auto it : registers_map) {
                    int leftCount = it.second;
                    for (int i=0;i < leftCount;i++) {
                        this->registers.push_back(it.first);
                    }
                }
                std::sort( this->registers.begin(), this->registers.end(),
                           [ ]( const Register& lhs, const Register& rhs ) {
                               return lhs < rhs;
                           });
                this->isMaterialized = true;
            }
            if (this->counter_index < static_cast<int>(this->registers.size())) {
                this->output_regs.push_back(this->registers[this->counter_index]);
                this->counter_index++;
                return true;
            } else {
                return false;
            }
        }


        std::vector<Register*> UnionAll::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        void UnionAll::close() {
            this->input_left->close();
            this->input_right->close();
        }


        Intersect::Intersect(Operator& input_left, Operator& input_right)
                : BinaryOperator(input_left, input_right) {

        }


        Intersect::~Intersect() = default;


        void Intersect::open() {
            this->input_left->open();
            this->input_right->open();
        }


        bool Intersect::next() {
            if (!this->isMaterialized) {
                std::unordered_map<Register, int, RegisterHasher> left_registers;
                std::unordered_map<Register, int, RegisterHasher> right_registers;
                while (this->input_left->next()) {
                    std::vector<Register*> regs = this->input_left->get_output();
                    for (auto it : regs) {
                        auto got = left_registers.find(*it);
                        if (got == left_registers.end()) {
                            left_registers.insert({*it, 1});
                        } else {
                            left_registers[*it] += 1;
                        }
                    }
                }
                /*std::cout << "left side" << std::endl;
                for (auto it : left_registers)
                    std::cout << " " << it.first.get_hash() << ":" << it.second;

                std::cout << '\n';
                std::cout << '\n' << "left side end" << std::endl;*/
                while (this->input_right->next()) {
                    std::vector<Register*> regs = this->input_right->get_output();
                    for (auto it : regs) {
                        auto got = right_registers.find(*it);
                        if (got == right_registers.end()) {
                            right_registers.insert({*it, 1});
                        } else {
                            right_registers[*it] += 1;
                        }
                    }
                }
                /*std::cout << "right side" << std::endl;
                for (auto it : right_registers)
                    std::cout << " " << it.first.get_hash() << ":" << it.second;

                std::cout << '\n';
                std::cout << '\n' << "right side end" << std::endl;*/

                for (auto it : left_registers) {
                    auto got = right_registers.find(it.first);
                    if (got != right_registers.end()) {
                        this->registers.push_back(it.first);
                    }
                }
                std::sort( this->registers.begin(), this->registers.end(),
                           [ ]( const Register& lhs, const Register& rhs ) {
                               return lhs < rhs;
                           });
                this->isMaterialized = true;
            }
            if (this->counter_index < static_cast<int>(this->registers.size())) {
                this->output_regs.push_back(this->registers[this->counter_index]);
                this->counter_index++;
                return true;
            } else {
                return false;
            }
        }


        std::vector<Register*> Intersect::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        void Intersect::close() {
           this->input_left->close();
           this->input_right->close();
        }


        IntersectAll::IntersectAll(Operator& input_left, Operator& input_right)
                : BinaryOperator(input_left, input_right) {
        }


        IntersectAll::~IntersectAll() = default;


        void IntersectAll::open() {
           this->input_left->open();
           this->input_right->open();
        }


        bool IntersectAll::next() {
            if (!this->isMaterialized) {
                std::unordered_map<Register, int, RegisterHasher> left_registers;
                std::unordered_map<Register, int, RegisterHasher> right_registers;
                while (this->input_left->next()) {
                    std::vector<Register*> regs = this->input_left->get_output();
                    for (auto it : regs) {
                        auto got = left_registers.find(*it);
                        if (got == left_registers.end()) {
                            left_registers.insert({*it, 1});
                        } else {
                            left_registers[*it] += 1;
                        }
                    }
                }

                while (this->input_right->next()) {
                    std::vector<Register*> regs = this->input_right->get_output();
                    for (auto it : regs) {
                        auto got = right_registers.find(*it);
                        if (got == right_registers.end()) {
                            right_registers.insert({*it, 1});
                        } else {
                            right_registers[*it] += 1;
                        }
                    }
                }

                for (auto it : left_registers) {
                    auto got = right_registers.find(it.first);
                    if (got != right_registers.end()) {
                        int leftCount = it.second;
                        int rightCount = got->second;
                        if (leftCount <= rightCount) {
                            for (int i=0;i < leftCount;i++) {
                                this->registers.push_back(it.first);
                            }
                        } else {
                            for (int i=0;i < rightCount;i++) {
                                this->registers.push_back(got->first);
                            }
                        }
                    }
                }
                std::sort( this->registers.begin(), this->registers.end(),
                           [ ]( const Register& lhs, const Register& rhs ) {
                               return lhs < rhs;
                           });
                this->isMaterialized = true;
            }
            if (this->counter_index < static_cast<int>(this->registers.size())) {
                this->output_regs.push_back(this->registers[this->counter_index]);
                this->counter_index++;
                return true;
            } else {
                return false;
            }
        }


        std::vector<Register*> IntersectAll::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        void IntersectAll::close() {
            this->input_left->close();
            this->input_right->close();
        }


        Except::Except(Operator& input_left, Operator& input_right)
                : BinaryOperator(input_left, input_right) {
        }


        Except::~Except() = default;


        void Except::open() {
            this->input_left->open();
            this->input_right->open();
        }


        bool Except::next() {
            if (!this->isMaterialized) {
                std::unordered_map<Register, int, RegisterHasher> left_registers;
                std::unordered_map<Register, int, RegisterHasher> right_registers;
                while (this->input_left->next()) {
                    std::vector<Register*> regs = this->input_left->get_output();
                    for (auto it : regs) {
                        auto got = left_registers.find(*it);
                        if (got == left_registers.end()) {
                            left_registers.insert({*it, 1});
                        } else {
                            left_registers[*it] += 1;
                        }
                    }
                }

                while (this->input_right->next()) {
                    std::vector<Register*> regs = this->input_right->get_output();
                    for (auto it : regs) {
                        auto got = right_registers.find(*it);
                        if (got == right_registers.end()) {
                            right_registers.insert({*it, 1});
                        } else {
                            right_registers[*it] += 1;
                        }
                    }
                }

                for (auto it : left_registers) {
                    auto got = right_registers.find(it.first);
                    if (got == right_registers.end()) {
                        this->registers.push_back(it.first);
                    }
                }
                std::sort( this->registers.begin(), this->registers.end(),
                           [ ]( const Register& lhs, const Register& rhs ) {
                               return lhs < rhs;
                           });
                this->isMaterialized = true;
            }
            if (this->counter_index < static_cast<int>(this->registers.size())) {
                this->output_regs.push_back(this->registers[this->counter_index]);
                this->counter_index++;
                return true;
            } else {
                return false;
            }
        }


        std::vector<Register*> Except::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        void Except::close() {
            this->input_left->close();
            this->input_right->close();
        }


        ExceptAll::ExceptAll(Operator& input_left, Operator& input_right)
                : BinaryOperator(input_left, input_right) {
        }


        ExceptAll::~ExceptAll() = default;


        void ExceptAll::open() {
            this->input_left->open();
            this->input_right->open();
        }


        bool ExceptAll::next() {
            if (!this->isMaterialized) {
                std::unordered_map<Register, int, RegisterHasher> left_registers;
                std::unordered_map<Register, int, RegisterHasher> right_registers;
                while (this->input_left->next()) {
                    std::vector<Register*> regs = this->input_left->get_output();
                    for (auto it : regs) {
                        auto got = left_registers.find(*it);
                        if (got == left_registers.end()) {
                            left_registers.insert({*it, 1});
                        } else {
                            left_registers[*it] += 1;
                        }
                    }
                }

                while (this->input_right->next()) {
                    std::vector<Register*> regs = this->input_right->get_output();
                    for (auto it : regs) {
                        auto got = right_registers.find(*it);
                        if (got == right_registers.end()) {
                            right_registers.insert({*it, 1});
                        } else {
                            right_registers[*it] += 1;
                        }
                    }
                }

                for (auto it : left_registers) {
                    auto got = right_registers.find(it.first);
                    if (got == right_registers.end()) {
                        int leftCount = it.second;
                        for (int i=0;i < leftCount;i++) {
                            this->registers.push_back(it.first);
                        }
                    } else {
                        int leftCount = it.second;
                        int rightCount = got->second;
                        if ((leftCount - rightCount) > 0) {
                            for (int i=0;i < (leftCount - rightCount) ;i++) {
                                this->registers.push_back(it.first);
                            }
                        }
                    }
                }
                std::sort( this->registers.begin(), this->registers.end(),
                           [ ]( const Register& lhs, const Register& rhs ) {
                               return lhs < rhs;
                           });
                this->isMaterialized = true;
            }
            if (this->counter_index < static_cast<int>(this->registers.size())) {
                this->output_regs.push_back(this->registers[this->counter_index]);
                this->counter_index++;
                return true;
            } else {
                return false;
            }
        }


        std::vector<Register*> ExceptAll::get_output() {
            std::vector<Register*> output;
            for (auto& reg : this->output_regs) {
                output.push_back(&reg);
            }
            this->output_regs.clear();
            return output;
        }


        void ExceptAll::close() {
            this->input_left->close();
            this->input_right->close();
        }

    }  // namespace iterator_model
}  // namespace moderndbs
