Create custom mutations using Actions
=====================================


.. contents:: Table of contents
  :backlinks: none
  :depth: 1
  :local:

Actions are user defined mutations with custom business logic. The first step towards creating an action is to define the various types and fields that you want in your schema.

Example
-------

A simple example of creating a user (all scalar input types)

.. code-block:: graphql

   mutation place_order($order_input: place_order_input!) {
     place_order(input: $order_input) {
       action_id
       response {
         order_id
       }
     }
   }



A complicated example of creating an author with articles (few scalar, few nested types)

.. code-block:: graphql

   mutation place_order($order_input: place_order_input!) {
     place_order(input: $order_input) {
       action_id
       response {
         order_id
       }
     }
   }
