#ifndef HPHP_ENIGMA_COMMON_H_H
#define HPHP_ENIGMA_COMMON_H_H

#if defined(ENIGMA_DEBUG)
#define ENIG_DEBUG(x) LOG(INFO) << x
#else
#define ENIG_DEBUG(x)
#endif

#define ENIGMA_ME(cn,fn) Native::registerBuiltinFunction("Enigma\\" #cn "->" #fn, HHVM_MN(cn,fn))
#define ENIGMA_NAMED_ME(cls,cn,fn) Native::registerBuiltinFunction("Enigma\\" #cn "->" #fn, HHVM_MN(cls,fn))

#endif //HPHP_ENIGMA_COMMON_H_H
