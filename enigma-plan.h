#ifndef HPHP_ENIGMA_PLAN_H
#define HPHP_ENIGMA_PLAN_H

#include "hphp/runtime/ext/extension.h"
#include <folly/EvictingCacheMap.h>
#include "enigma-common.h"

namespace HPHP {
namespace Enigma {


struct PlanInfo {
    enum class ParameterType {
        /*
         * Query uses the numbered prepared parameter placeholder.
         * (eg. "where id = ?")
         */
                Numbered,
        /*
         * Query uses the named prepared parameter placeholder.
         * (eg. "where id = :id")
         */
                Named
    };

    PlanInfo(std::string const & cmd);

    Array mapParameters(Array const & params) const;

    std::string command;
    std::string rewrittenCommand;
    ParameterType type;
    std::vector<std::string> parameterNameMap;
    unsigned parameterCount;

private:
    Array mapNamedParameters(Array const & params) const;
    Array mapNumberedParameters(Array const & params) const;

    void determineParameterType();
    std::tuple<std::string, unsigned> parseNumberedParameters() const;
    std::tuple<std::string, std::vector<std::string> > parseNamedParameters() const;
    bool isValidPlaceholder(std::size_t pos) const;
    bool isValidNamedPlaceholder(std::size_t pos) const;
    std::size_t namedPlaceholderLength(std::size_t pos) const;
};

class PlanCache {
public:
    struct CachedPlan {
        CachedPlan(std::string const & cmd);

        std::string statementName;
        PlanInfo planInfo;
    };

    typedef std::unique_ptr<CachedPlan> p_CachedPlan;
    static const unsigned DefaultPlanCacheSize = 30;
    static const unsigned MaxPlanCacheSize = 1000;

    PlanCache(unsigned size = DefaultPlanCacheSize);

    PlanCache(PlanCache const &) = delete;
    PlanCache & operator = (PlanCache const &) = delete;

    CachedPlan const * lookupPlan(std::string const & query);
    CachedPlan const * assignPlan(std::string const & query);
    void forgetPlan(std::string const & query);
    void clear();

private:
    static constexpr char const * PlanNamePrefix = "EnigmaPlan_";

    unsigned nextPlanId_{0};
    folly::EvictingCacheMap<std::string, p_CachedPlan> plans_;

    CachedPlan const * storePlan(std::string const & query, std::string const & statementName);
    std::string generatePlanName();
};

typedef std::unique_ptr<PlanCache> p_PlanCache;

}
}

#endif //HPHP_ENIGMA_PLAN_H
