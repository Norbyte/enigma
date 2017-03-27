#include <hphp/util/conv-10.h>
#include "enigma-queue.h"
#include "enigma-transaction.h"
#include "hphp/runtime/vm/native-data.h"

namespace HPHP {
namespace Enigma {


PlanInfo::PlanInfo(std::string const & cmd)
        : command(cmd) {
    determineParameterType();
}

Array PlanInfo::mapParameters(Array const & params) const {
    if (type == ParameterType::Named) {
        return mapNamedParameters(params);
    } else {
        return mapNumberedParameters(params);
    }
}

Array PlanInfo::mapNamedParameters(Array const & params) const {
    if (parameterNameMap.size() != params.size()) {
        throw Exception(std::string("Parameter count mismatch; expected ") + std::to_string(parameterNameMap.size())
                        + " named parameters, got " + std::to_string(params.size()));
    }

    Array mapped{Array::Create()};
    for (unsigned i = 0; i < parameterNameMap.size(); i++) {
        auto key = String(parameterNameMap[i]);
        auto value = params->nvGet(key.get());
        if (value == nullptr) {
            throw EnigmaException(std::string("Missing bound parameter: ") + key.c_str());
        }

        mapped.append(*reinterpret_cast<Variant const *>(value));
    }

    return mapped;
}

Array PlanInfo::mapNumberedParameters(Array const & params) const {
    if (parameterCount != params.size()) {
        throw Exception(std::string("Parameter count mismatch; expected ") + std::to_string(parameterCount)
                        + " parameters, got " + std::to_string(params.size()));
    }

    Array mapped{Array::attach(PackedArray::MakeReserve(parameterCount))};
    for (unsigned i = 0; i < parameterCount; i++) {
        auto value = params->nvGet(i);
        if (value == nullptr) {
            throw Exception(std::string("Missing bound parameter: ") + std::to_string(i));
        }

        mapped.append(*reinterpret_cast<Variant const *>(value));
    }

    return mapped;
}

void PlanInfo::determineParameterType() {
    auto numbered = parseNumberedParameters();
    auto named = parseNamedParameters();

    if (std::get<1>(named).size() > 0 && std::get<1>(numbered) > 0) {
        throw Exception("Query contains both named and numbered parameters");
    }

    if (std::get<1>(named).size() > 0) {
        type = ParameterType::Named;
        rewrittenCommand = std::move(std::get<0>(named));
        parameterNameMap = std::move(std::get<1>(named));
    } else {
        type = ParameterType::Numbered;
        rewrittenCommand = std::move(std::get<0>(numbered));
        parameterCount = std::get<1>(numbered);
    }
}

inline bool is_space(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

inline bool is_alnum(char c) {
    return (c >= '0' && c <= '9')
           || (c >= 'a' && c <= 'z')
           || (c >= 'A' && c <= 'Z');
}

inline bool is_placeholder_char(char c) {
    return is_alnum(c) || c == '_';
}

inline bool PlanInfo::isValidPlaceholder(std::size_t pos) const {
    // Check if the preceding byte is in [0-9a-zA-Z (\r\n\t]
    if (pos != 0) {
        char prev = command[pos - 1];
        if (
                !is_space(prev)
                && !is_alnum(prev)
                && prev != '('
                && prev != ']'
                && prev != ','
                ) {
            return false;
        }
    }

    // Check if the following byte is in [0-9a-zA-Z:) \r\n\t]
    // Allow ":", as parameter typecasting is fairly common
    if (pos < command.length() - 1) {
        char next = command[pos + 1];
        if (
                !is_space(next)
                && !is_alnum(next)
                && next != ':'
                && next != ')'
                && next != ']'
                && next != ','
                ) {
            return false;
        }
    }

    return true;
}

inline bool PlanInfo::isValidNamedPlaceholder(std::size_t pos) const {
    // Check if the preceding byte is in [ \r\n\t]
    if (pos != 0) {
        char prev = command[pos - 1];
        if (
                !is_space(prev)
                && prev != '('
                && prev != '['
                && prev != ','
                ) {
            return false;
        }
    }

    // Check if the following byte is in [0-9a-zA-Z_]
    if (
            pos < command.length() - 1
            && !is_placeholder_char(command[pos + 1])
            ) {
        return false;
    }

    return true;
}

inline std::size_t PlanInfo::namedPlaceholderLength(std::size_t pos) const {
    if (!isValidNamedPlaceholder(pos)) {
        return 0;
    }

    auto i = pos + 1;
    for (; i < command.length() && (is_alnum(command[i]) || command[i] == '_'); i++);

    return i - pos - 1;
}

std::tuple<std::string, unsigned> __attribute__ ((noinline)) PlanInfo::parseNumberedParameters() const {
    unsigned numParams{0};
    std::string rewritten;
    rewritten.reserve(command.size() + (command.size() >> 1));

    // Check if the placeholder is valid
    // (should only be preceded and followed by [0-9a-z] and whitespace)
    std::size_t pos{0}, lastWrittenPos{0};
    for (;;) {
        pos = command.find('?', pos);
        if (pos == std::string::npos) {
            break;
        }

        rewritten.append(command.data() + lastWrittenPos, pos - lastWrittenPos);
        lastWrittenPos = pos + 1;

        if (isValidPlaceholder(pos++)) {
            char paramNo[21];
            paramNo[0] = '\0';
            auto paramNoStr = conv_10(++numParams, &paramNo[20]);
            rewritten.push_back('$');
            rewritten.append(paramNoStr.start(), paramNoStr.size());
        } else {
            rewritten.push_back('?');
        }
    }

    rewritten.append(command.data() + lastWrittenPos, command.length() - lastWrittenPos);
    return std::make_tuple(rewritten, numParams);
}

std::tuple<std::string, std::vector<std::string> > PlanInfo::parseNamedParameters() const {
    std::vector<std::string> params;
    std::unordered_map<std::string, unsigned> paramMap;
    std::string rewritten;
    rewritten.reserve(command.size() + (command.size() >> 1));

    std::size_t pos{0}, lastWrittenPos{0};
    for (;;) {
        pos = command.find(':', pos);
        if (pos == std::string::npos) {
            break;
        }

        rewritten.append(command.data() + lastWrittenPos, pos - lastWrittenPos);
        lastWrittenPos = pos + 1;

        auto placeholderLength = namedPlaceholderLength(pos++);
        if (placeholderLength > 0) {
            std::string param = command.substr(pos, placeholderLength);
            auto paramIt = paramMap.find(param);
            if (paramIt != paramMap.end()) {
                char paramNo[21];
                paramNo[0] = '\0';
                auto paramNoStr = conv_10(paramIt->second, &paramNo[20]);
                rewritten.push_back('$');
                rewritten.append(paramNoStr.start(), paramNoStr.size());
            } else {
                params.push_back(param);
                paramMap.insert(std::make_pair(param, params.size()));

                char paramNo[21];
                paramNo[0] = '\0';
                auto paramNoStr = conv_10(params.size(), &paramNo[20]);
                rewritten.push_back('$');
                rewritten.append(paramNoStr.start(), paramNoStr.size());
            }

            pos += placeholderLength;
            lastWrittenPos += placeholderLength;
        } else {
            rewritten.push_back(':');
        }
    }

    rewritten.append(command.data() + lastWrittenPos, command.length() - lastWrittenPos);
    return std::make_tuple(rewritten, std::move(params));
}

PlanCache::PlanCache(unsigned size)
        : plans_(size)
{}

PlanCache::CachedPlan::CachedPlan(std::string const & cmd)
        : planInfo(cmd)
{}

PlanCache::CachedPlan const * PlanCache::lookupPlan(std::string const & query) {
    auto it = plans_.find(query);
    if (it != plans_.end()) {
        return it->second.get();
    } else {
        return nullptr;
    }
}

PlanCache::CachedPlan const * PlanCache::assignPlan(std::string const & query) {
    auto name = generatePlanName();
    return storePlan(query, name);
}

void PlanCache::forgetPlan(std::string const & query) {
    plans_.erase(query);
}

PlanCache::CachedPlan const * PlanCache::storePlan(std::string const & query, std::string const & statementName) {
    auto plan = p_CachedPlan(new CachedPlan(query));
    auto planPtr = plan.get();
    plan->statementName = statementName;
    plans_.set(query, std::move(plan));
    return planPtr;
}

void PlanCache::clear() {
    plans_.clear();
}

std::string PlanCache::generatePlanName() {
    return PlanNamePrefix + std::to_string(nextPlanId_++);
}

}
}
