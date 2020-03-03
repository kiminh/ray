
// #include <memory>
// #include <utility>
// #include <ray/api/ray_object.h>
// #include <ray/api/uniqueId.h>
// #include <ray/api.h>

// namespace ray {

// template <typename T>
// RayObject<T>::RayObject() {}

// template <typename T>
// RayObject<T>::RayObject(const UniqueId &id) {
//   _id = id;
// }

// template <typename T>
// RayObject<T>::RayObject(const UniqueId &&id) {
//   _id = std::move(id);
// }

// template <typename T>
// void RayObject<T>::assign(const UniqueId &id) {
//   _id = id;
// }

// template <typename T>
// void RayObject<T>::assign(UniqueId &&id) {
//   _id = std::move(id);
// }

// template <typename T>
// const UniqueId &RayObject<T>::id() const {
//   return _id;
// }

// template <typename T>
// inline std::shared_ptr<T> RayObject<T>::get() const {
//   const RayObject<T> object(_id);
//   return Ray::get(object);
// }

// // template <typename T>
// // template <typename TO>
// // inline std::shared_ptr<T> RayObject<T>::doGet() const {
// //   std::unique_ptr<TO> pObj(new TO);
// //   Ray::get(_id, *pObj);
// //   return pObj;
// // }

// template <typename T>
// inline bool RayObject<T>::operator==(const RayObject<T> &object) const {
//   if (_id == object.id()) {
//     return true;
//   } else {
//     return false;
//   }
// }

// }  // namespace ray