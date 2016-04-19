#include "hphp/runtime/ext/extension.h"
#include "enigma-query.h"
#include "enigma-queue.h"
#include "hphp/runtime/vm/native-data.h"

namespace HPHP {

    const StaticString s_EnigmaQueryResult("EnigmaQueryResult");

    Object HHVM_FUNCTION(enigma_create_pool, Array const & options) {
        auto pool = new EnigmaPool(options);
        auto poolInterface = EnigmaPoolInterface::newInstance(pool);
        return poolInterface;
    }


    class EnigmaExtension : public Extension {
    public:
        EnigmaExtension() : Extension("enigma", "1.0") { }

        void moduleInit() override {
            HHVM_FE(enigma_create_pool);
            registerEnigmaClasses();
            registerEnigmaQueueClasses();
            loadSystemlib();
        }
    } s_enigma_extension;

    HHVM_GET_MODULE(enigma);

}
