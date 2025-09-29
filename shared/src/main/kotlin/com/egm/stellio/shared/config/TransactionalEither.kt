import arrow.core.Either
import org.aopalliance.intercept.MethodInterceptor
import org.aopalliance.intercept.MethodInvocation
import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionStatus
import org.springframework.transaction.support.DefaultTransactionDefinition

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class TransactionalEither

@Component
class TransactionalEitherInterceptor(
    private val transactionManager: PlatformTransactionManager
) : MethodInterceptor {

    override fun invoke(invocation: MethodInvocation): Any? {
        val method = invocation.method
        if (!method.isAnnotationPresent(TransactionalEither::class.java)) {
            return invocation.proceed()
        }

        val txStatus: TransactionStatus = transactionManager.getTransaction(DefaultTransactionDefinition())

        return try {
            val result = invocation.proceed()

            if (result is Either<*, *>) {
                if (result.isLeft()) {
                    transactionManager.rollback(txStatus)
                    println("Transaction rolled back because result is Left")
                } else {
                    transactionManager.commit(txStatus)
                }
            } else {
                transactionManager.commit(txStatus)
            }

            result
        } catch (ex: Exception) {
            transactionManager.rollback(txStatus)
            throw ex
        }
    }
}
