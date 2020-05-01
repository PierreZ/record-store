package fr.pierrezemb.recordstore.auth;

import static com.clevercloud.biscuit.token.builder.Utils.caveat;
import static com.clevercloud.biscuit.token.builder.Utils.fact;
import static com.clevercloud.biscuit.token.builder.Utils.pred;
import static com.clevercloud.biscuit.token.builder.Utils.rule;
import static com.clevercloud.biscuit.token.builder.Utils.s;
import static com.clevercloud.biscuit.token.builder.Utils.string;
import static com.clevercloud.biscuit.token.builder.Utils.var;
import static io.vavr.API.Left;
import static io.vavr.API.Right;

import com.clevercloud.biscuit.crypto.KeyPair;
import com.clevercloud.biscuit.datalog.SymbolTable;
import com.clevercloud.biscuit.error.Error;
import com.clevercloud.biscuit.token.Biscuit;
import com.clevercloud.biscuit.token.Verifier;
import com.clevercloud.biscuit.token.builder.Block;
import io.vavr.control.Either;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BiscuitManager {
  public static final String DEFAULT_BISCUIT_KEY = "3A8621F1847F19D6DAEAB5465CE8D3908B91C66FB9AF380D508FCF9253458907";
  private static final Logger LOGGER = LoggerFactory.getLogger(BiscuitManager.class);
  private final SymbolTable symbols;
  KeyPair root;
  byte[] seed = {0, 0, 0, 0};
  SecureRandom rng = new SecureRandom(seed);

  public BiscuitManager(String key) {
    root = new KeyPair(key);
    symbols = Biscuit.default_symbol_table();
  }

  public BiscuitManager() {
    this(DEFAULT_BISCUIT_KEY);
  }

  public String create(String tenant, List<String> authorizedContainers) {
    Block authority_builder = new Block(0, symbols);

    // add tenant fact in biscuit
    authority_builder.add_fact(
      fact("right", Arrays.asList(s("authority"), s("tenant"), s(tenant))));

    // add containers in biscuit
    for (String s : authorizedContainers) {
      authority_builder.add_fact(fact("right", Arrays.asList(s("authority"), s("container"), s(s))));
    }

    // TODO: handle error in a nicer way
    return Base64.getEncoder()
      .encodeToString(
        Biscuit.make(rng, root, Biscuit.default_symbol_table(), authority_builder.build())
          .get().seal(root.private_key.toByteArray()).get());
  }

  public Either<Error, Void> checkTenant(String tenant, String serializedBiscuit) {

    Either<Error, Verifier> res = createVerifier(serializedBiscuit);
    if (res.isLeft()) {
      LOGGER.error("could not create verifier: {}", res.getLeft());
      return Left(res.getLeft());
    }

    Verifier verifier = res.get();
    verifier.add_fact(fact("tenant", Arrays.asList(s("ambient"), s(tenant))));
    verifier.set_time();

    verifier.add_caveat(caveat(rule(
      "checked_tenant_right",
      Arrays.asList(string(tenant)),
      Arrays.asList(pred("right", Arrays.asList(s("authority"), s("tenant"), s(tenant))))
    )));

    return verifier.verify();
  }

  public Either<Error, Verifier> createVerifier(String serializedBiscuit) {

    Either<Error, Biscuit> deser = Biscuit.from_sealed(
      Base64.getDecoder().decode(serializedBiscuit),
      root.private_key.toByteArray()
    );
    if (deser.isLeft()) {
      Error.FormatError e = (Error.FormatError) deser.getLeft();
      LOGGER.error("cannot deserialize biscuit: {}", e.toString());

      return Left(e);
    }

    Biscuit token = deser.get();

    Either<Error, Verifier> res = token.verify_sealed();
    if (res.isLeft()) {
      return res;
    }

    Verifier verifier = res.get();
    verifier.add_rule(rule(
      "right",
      Arrays.asList(s("authority"), s("tenant"), var(0)),
      Arrays.asList(pred("right", Arrays.asList(s("authority"), s("tenant"), var(0))))));

    return Right(verifier);
  }

}
