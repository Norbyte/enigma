#include "hphp/runtime/ext/extension.h"
#include "enigma-query.h"
#include "enigma-queue.h"
#include "hphp/runtime/vm/native-data.h"

namespace HPHP {

    namespace {
        Enigma::PersistentPoolStorage s_pools;
    }

    const StaticString
        s_Persistent("persistent");

    Object HHVM_FUNCTION(create_pool, Array const & connectionOpts, Array const & poolOpts) {
        bool persistent = false;
        if (poolOpts.exists(s_Persistent)) {
            persistent = poolOpts[s_Persistent].toBoolean();
        }

        Object poolHandle;
        if (persistent) {
            auto pool = s_pools.make(connectionOpts, poolOpts);
            poolHandle = Enigma::PoolHandle::newInstance(pool);
        } else {
            auto pool = std::make_shared<Enigma::Pool>(connectionOpts, poolOpts);
            poolHandle = Enigma::PoolHandle::newInstance(pool);
        }

        return poolHandle;
    }


    class EnigmaExtension : public Extension {
    public:
        EnigmaExtension() : Extension("enigma", "1.0") { }

        void moduleInit() override {
            Native::registerBuiltinFunction("Enigma\\create_pool", HHVM_FN(create_pool));
            Enigma::registerClasses();
            Enigma::registerQueueClasses();
            loadSystemlib();
        }
    } s_enigma_extension;

    HHVM_GET_MODULE(enigma);

}
