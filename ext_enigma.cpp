#include "hphp/runtime/ext/extension.h"
#include "enigma-query.h"
#include "enigma-queue.h"
#include "hphp/runtime/vm/native-data.h"

namespace HPHP {

    Object HHVM_FUNCTION(create_pool, Array const & options) {
        auto pool = new Enigma::Pool(options);
        auto poolInterface = Enigma::PoolInterface::newInstance(pool);
        return poolInterface;
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
